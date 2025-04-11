import os
import time
import threading
import json
from datetime import datetime
import logging
import random
from urllib.parse import urlparse, parse_qs
import asyncio
from functools import wraps
import queue # Импорт очереди
import concurrent.futures # Импорт для ThreadPoolExecutor

from flask import Flask, render_template, request, redirect, url_for
# --- Selenium Imports ---
from selenium.webdriver.chrome.options import Options as SeleniumOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
# ИСПРАВЛЕНО: Убраны импорты urllib3 и connection_error из selenium.common.exceptions
from selenium.common.exceptions import (
    TimeoutException, WebDriverException, NoSuchElementException,
    NoSuchWindowException
)
# --- undetected-chromedriver Import ---
import undetected_chromedriver as uc
# --- Other Imports ---
import requests
from requests.exceptions import RequestException
import urllib3 # <--- ДОБАВЛЕН ИМПОРТ urllib3
# Исключения urllib3, которые могут возникать при проблемах с соединением
from urllib3.exceptions import MaxRetryError, NewConnectionError, ProtocolError
# Стандартные исключения Python для сети/ОС
from socket import gaierror as SocketGaiError # Ошибка разрешения DNS
# ConnectionRefusedError ловится через OSError

# --- Telegram Bot Imports ---
import telegram
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters,
    ConversationHandler, CallbackQueryHandler
)
from telegram.constants import ParseMode

# --- Настройки ---
# !!! ОСНОВНЫЕ ПАРАМЕТРЫ !!!
CHECK_INTERVAL_SECONDS = 90  # Интервал между циклами проверки
MAX_WORKERS = 4              # ОПТИМАЛЬНОЕ КОЛ-ВО ПОТОКОВ (подберите для вашей системы, начните с 3-5)
MAX_HISTORY_PER_URL = 50
MAX_TELEGRAM_USERS = 10
TELEGRAM_BOT_TOKEN = "7359502748:AAHwLgsw7lZ0-dkvNtHj2cFk6m0m-eHtxJY" # ЗАМЕНИТЕ НА СВОЙ!

# --- Файлы ---
HISTORY_FILE = "price_history.json"
URL_FILE = "urls.json"
TELEGRAM_USERS_FILE = "telegram_users.json"

# --- Настройки Selenium/UC ---
PAGE_LOAD_TIMEOUT = 150      # Таймаут загрузки страницы
ELEMENT_WAIT_TIMEOUT = 60    # Таймаут ожидания цены/заголовка
TITLE_WAIT_TIMEOUT = 20      # Таймаут ожидания заголовка
# --- Селекторы (ПРОВЕРЬТЕ АКТУАЛЬНОСТЬ!) ---
MAIN_PRICE_SELECTOR = ".product-buy__price_active" # Селектор АКТУАЛЬНОЙ цены
TITLE_SELECTOR = "h1.product-card-top__title"

# --- Настройки User-Agent ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# --- Настройки Имитации Пользователя ---
ENABLE_USER_SIMULATION = True # Включить случайные действия (скролл, мышь)
MIN_ACTION_DELAY = 0.5
MAX_ACTION_DELAY = 1.5

# --- Состояния для ConversationHandler ---
STATE_WAITING_URL_TO_ADD = 1

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)
logging.getLogger("selenium.webdriver.remote.remote_connection").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.INFO)
logging.getLogger("undetected_chromedriver").setLevel(logging.WARNING)

# --- Инициализация Flask ---
app = Flask(__name__)

# --- Глобальные переменные ---
driver_init_lock = threading.Lock() # Лок для инициализации драйвера

app_state = {
    "products": {},
    "urls_to_monitor": [],
    "telegram_users": set(),
    "lock": threading.Lock(), # Общий лок для данных приложения
    "telegram_app": None,
    "message_queue": queue.Queue() # Очередь для сообщений Telegram
}

# --- Функции Загрузки/Сохранения ---
# Функция load_proxies() удалена
# Остальные функции load/save без изменений...
def load_urls():
    global app_state
    urls = []
    if os.path.exists(URL_FILE):
        try:
            if os.path.getsize(URL_FILE) > 0:
                with open(URL_FILE, 'r', encoding='utf-8') as f: loaded_urls = json.load(f)
                if isinstance(loaded_urls, list):
                    urls = [u for u in loaded_urls if isinstance(u, str) and urlparse(u).scheme in ['http', 'https'] and 'dns-shop.ru' in urlparse(u).netloc]
                    logging.info(f"Загружено {len(urls)} валидных URL из {URL_FILE}")
                else: logging.error(f"Содержимое файла {URL_FILE} не является списком.")
            else: logging.warning(f"Файл {URL_FILE} пустой.")
        except json.JSONDecodeError as e: logging.error(f"Ошибка декодирования JSON в {URL_FILE}: {e}")
        except Exception as e: logging.error(f"Ошибка чтения {URL_FILE}: {e}")
    else: logging.info(f"Файл {URL_FILE} не найден.")

    with app_state["lock"]:
        app_state["urls_to_monitor"] = urls
        current_urls_set = set(urls)
        for url in list(app_state["products"].keys()):
            if url not in current_urls_set:
                logging.info(f"Удаление данных URL: {url[:60]}...")
                del app_state["products"][url]

def save_urls():
    global app_state
    urls_copy = []
    try:
        with app_state["lock"]: urls_copy = list(app_state["urls_to_monitor"])
        with open(URL_FILE, 'w', encoding='utf-8') as f: json.dump(urls_copy, f, ensure_ascii=False, indent=4)
        logging.info(f"Список URL ({len(urls_copy)} шт.) сохранен в {URL_FILE}")
    except Exception as e: logging.error(f"Не удалось сохранить URL в {URL_FILE}: {e}")

def load_price_history():
    global app_state
    history_data = {}
    if os.path.exists(HISTORY_FILE):
        try:
            if os.path.getsize(HISTORY_FILE) > 0:
                with open(HISTORY_FILE, 'r', encoding='utf-8') as f: loaded_data = json.load(f)
                if isinstance(loaded_data, dict):
                    history_data = loaded_data
                    logging.info(f"Загружена история для {len(history_data)} URL из {HISTORY_FILE}")
                else: logging.error(f"Содержимое файла {HISTORY_FILE} не является словарем.")
            else: logging.warning(f"Файл {HISTORY_FILE} пустой.")
        except json.JSONDecodeError as e: logging.error(f"Ошибка декодирования JSON в {HISTORY_FILE}: {e}")
        except Exception as e: logging.error(f"Ошибка загрузки истории из {HISTORY_FILE}: {e}")
    else: logging.info(f"Файл {HISTORY_FILE} не найден.")

    with app_state["lock"]:
        current_urls = set(app_state.get("urls_to_monitor", []))
        for url in current_urls:
            if url not in app_state["products"]: app_state["products"][url] = {}
            loaded_hist = history_data.get(url, [])
            app_state["products"][url]["history"] = loaded_hist if isinstance(loaded_hist, list) else []
        for url in list(app_state["products"].keys()):
             if url not in current_urls:
                 logging.warning(f"Удаление истории для URL: {url[:60]}...")
                 del app_state["products"][url]
        logging.info(f"История цен синхронизирована для {len(app_state['products'])} URL.")

def save_price_history():
    global app_state
    history_to_save = {}
    try:
        with app_state["lock"]:
            active_urls = set(app_state.get("urls_to_monitor", []))
            for url, data in app_state["products"].items():
                if url in active_urls and "history" in data and isinstance(data["history"], list):
                    history_to_save[url] = list(data["history"])
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f: json.dump(history_to_save, f, ensure_ascii=False, indent=4)
        logging.info(f"История цен ({len(history_to_save)} URL) сохранена в {HISTORY_FILE}.")
    except Exception as e: logging.error(f"Не удалось сохранить историю цен: {e}")

def load_telegram_users():
    global app_state
    users = set()
    if os.path.exists(TELEGRAM_USERS_FILE):
        try:
            if os.path.getsize(TELEGRAM_USERS_FILE) > 0:
                with open(TELEGRAM_USERS_FILE, 'r', encoding='utf-8') as f: loaded_users = json.load(f)
                if isinstance(loaded_users, list):
                    users = {int(u) for u in loaded_users if isinstance(u, (int, str)) and str(u).isdigit()}
                    logging.info(f"Загружено {len(users)} chat_id из {TELEGRAM_USERS_FILE}")
                else: logging.error(f"Содержимое файла {TELEGRAM_USERS_FILE} не является списком.")
            else: logging.warning(f"Файл {TELEGRAM_USERS_FILE} пустой.")
        except json.JSONDecodeError as e: logging.error(f"Ошибка декодирования JSON в {TELEGRAM_USERS_FILE}: {e}")
        except Exception as e: logging.error(f"Ошибка чтения {TELEGRAM_USERS_FILE}: {e}")
    else: logging.info(f"Файл {TELEGRAM_USERS_FILE} не найден.")
    with app_state["lock"]: app_state["telegram_users"] = users

def save_telegram_users():
    global app_state
    users_copy = set()
    try:
        with app_state["lock"]: users_copy = set(app_state["telegram_users"])
        with open(TELEGRAM_USERS_FILE, 'w', encoding='utf-8') as f: json.dump(list(users_copy), f, ensure_ascii=False, indent=4)
        logging.info(f"Список пользователей Telegram ({len(users_copy)} шт.) сохранен в {TELEGRAM_USERS_FILE}")
    except Exception as e: logging.error(f"Не удалось сохранить список TG: {e}")


# --- Основная Функция Парсинга (БЕЗ TOR, ищет актуальную цену) ---
def get_price_from_dns(url):
    """
    Получает цену и название товара с DNS, используя undetected_chromedriver.
    Применяет ротацию User-Agent и имитацию действий.
    Ищет актуальную цену по точному селектору и извлекает её через JS.
    """
    global driver_init_lock
    price = None
    product_name = None
    error_message = None
    driver = None
    options = SeleniumOptions()
    thread_name = threading.current_thread().name
    url_snippet = url[:60] + "..." if len(url) > 60 else url

    # --- Настройка опций Chrome ---
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--start-maximized")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-infobars')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-application-cache')
    options.add_argument('--disk-cache-size=0')
    options.add_argument('--clear-session-cache')

    # --- Ротация User-Agent ---
    random_user_agent = random.choice(USER_AGENTS)
    options.add_argument(f"user-agent={random_user_agent}")
    logging.debug(f"[{thread_name}] Используется User-Agent: ...{random_user_agent[-50:]}")

    try:
        # --- Инициализация драйвера (под локом) ---
        logging.debug(f"[{thread_name}] Ожидание блокировки для инициализации драйвера ({url_snippet})...")
        with driver_init_lock:
            logging.info(f"[{thread_name}] Блокировка получена. Инициализация UC для URL: {url_snippet}...")
            driver = uc.Chrome(options=options, use_subprocess=True)
            logging.info(f"[{thread_name}] Инициализация UC завершена.")

        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

        logging.info(f"[{thread_name}] Загрузка страницы: {url_snippet}...")
        try:
            driver.get(url)
        # ИСПРАВЛЕНО: Ловим нужные исключения urllib3 и socket
        except (WebDriverException, ProtocolError, ConnectionResetError, MaxRetryError, NewConnectionError, SocketGaiError) as e_get:
             error_message = f"Ошибка при driver.get(): {e_get.__class__.__name__}"
             logging.error(f"[{thread_name}] {error_message}: {str(e_get)[:200]}...", exc_info=False)
             if driver: 
                 try: 
                     driver.quit() 
                 except: 
                     pass
             return url, None, "Название не получено (ошибка загрузки)", error_message

        # --- Имитация действий пользователя (опционально) ---
        if ENABLE_USER_SIMULATION and error_message is None:
            try:
                logging.debug(f"[{thread_name}] Имитация действий пользователя...")
                actions = ActionChains(driver)
                viewport_height = driver.execute_script("return window.innerHeight")
                scroll_attempts = random.randint(1, 3)
                for i in range(scroll_attempts):
                    scroll_y = random.randint(int(viewport_height * 0.4), int(viewport_height * 0.9))
                    driver.execute_script(f"window.scrollBy(0, {scroll_y});")
                    logging.debug(f"[{thread_name}] Скролл вниз {i+1}/{scroll_attempts}")
                    time.sleep(random.uniform(MIN_ACTION_DELAY, MAX_ACTION_DELAY))
                    if i < scroll_attempts - 1 and random.random() < 0.4:
                         scroll_y_up = random.randint(int(viewport_height * 0.1), int(viewport_height * 0.3))
                         driver.execute_script(f"window.scrollBy(0, -{scroll_y_up});")
                         logging.debug(f"[{thread_name}] Скролл вверх")
                         time.sleep(random.uniform(MIN_ACTION_DELAY / 2, MAX_ACTION_DELAY / 2))
                if random.random() < 0.6:
                    try:
                        possible_targets = driver.find_elements(By.CSS_SELECTOR, "a, button, img.product-images-slider__img, .product-buy__option-text")
                        if possible_targets:
                            target_element = random.choice(possible_targets)
                            driver.execute_script("arguments[0].scrollIntoViewIfNeeded(true);", target_element)
                            time.sleep(0.2)
                            actions.move_to_element(target_element).pause(random.uniform(0.2, 0.6)).perform()
                            logging.debug(f"[{thread_name}] Имитация наведения мыши.")
                            time.sleep(random.uniform(MIN_ACTION_DELAY / 2, MAX_ACTION_DELAY / 2))
                    except Exception as e_mouse:
                        logging.warning(f"[{thread_name}] Небольшая ошибка имитации мыши: {e_mouse}", exc_info=False)
                logging.debug(f"[{thread_name}] Имитация действий завершена.")
            except (WebDriverException, MaxRetryError, NoSuchWindowException) as e_sim: # Исключаем ConnectionRefusedError, т.к. он в OSError
                 error_message = f"Ошибка при имитации действий: {e_sim.__class__.__name__}"
                 logging.error(f"[{thread_name}] {error_message}: {str(e_sim)[:200]}...", exc_info=False)
            except Exception as e_sim_other:
                 logging.error(f"[{thread_name}] Неожиданная ошибка имитации: {e_sim_other}", exc_info=True)

        # --- Поиск ЭЛЕМЕНТА С АКТУАЛЬНОЙ ЦЕНОЙ ---
        if error_message is None:
            try:
                price_wait_timeout = ELEMENT_WAIT_TIMEOUT
                logging.info(f"[{thread_name}] Ожидание элемента АКТУАЛЬНОЙ цены ({MAIN_PRICE_SELECTOR}) - до {price_wait_timeout} сек...")
                price_wait = WebDriverWait(driver, price_wait_timeout)
                price_element = price_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, MAIN_PRICE_SELECTOR)))
                logging.info(f"[{thread_name}] Элемент АКТУАЛЬНОЙ цены найден.")

                # --- Извлечение цены (JS) ---
                try:
                    script = r"""
                    var priceElement = arguments[0];
                    var priceText = '';
                    if (priceElement) {
                        var childNodes = priceElement.childNodes;
                        for (var i = 0; i < childNodes.length; i++) {
                            if (childNodes[i].nodeType === 3) { priceText += childNodes[i].textContent.trim(); }
                        }
                        if (!priceText.trim()) {
                           priceText = priceElement.textContent || priceElement.innerText || '';
                           var prevPriceElement = priceElement.querySelector('.product-buy__prev');
                           if (prevPriceElement) {
                               var prevPriceText = prevPriceElement.textContent || prevPriceElement.innerText || '';
                               if (prevPriceText && priceText.includes(prevPriceText)) { priceText = priceText.replace(prevPriceText, '').trim(); }
                           }
                        }
                    }
                    return priceText.replace(/[^\d]/g, '');
                    """
                    price_text_js = driver.execute_script(script, price_element)
                    logging.info(f"[{thread_name}] Очищенный текст цены (из JS): '{price_text_js}'")

                    if price_text_js:
                        try:
                            price = int(price_text_js)
                            logging.info(f"[{thread_name}] Актуальная цена извлечена (JS): {price}")
                        except ValueError:
                             error_message = f"Не удалось преобразовать '{price_text_js}' в число (JS)."
                             logging.error(f"[{thread_name}] {error_message}")
                    else:
                        error_message = "JS не вернул цифры для элемента актуальной цены."
                        logging.warning(f"[{thread_name}] {error_message}")

                except (WebDriverException, MaxRetryError, NoSuchWindowException) as e_js_interact: # Исключаем ConnectionRefusedError
                    error_message = f"Ошибка взаимодействия (JS): {e_js_interact.__class__.__name__}"
                    logging.error(f"[{thread_name}] {error_message}: {str(e_js_interact)[:200]}...", exc_info=False)
                except Exception as e_js:
                    error_message = f"Ошибка при извлечении цены через JS: {e_js}"
                    logging.error(f"[{thread_name}] {error_message}", exc_info=True)

            except TimeoutException:
                error_message = f"Таймаут ({price_wait_timeout} сек) ожидания элемента АКТУАЛЬНОЙ цены ({MAIN_PRICE_SELECTOR})."
                logging.error(f"[{thread_name}] {error_message}")
                try:
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_thread_name = ''.join(c if c.isalnum() else '_' for c in thread_name)
                    filename = f"error_screenshot_price_timeout_{safe_thread_name}_{ts}.png"
                    driver.save_screenshot(filename)
                    logging.info(f"[{thread_name}] Скриншот сохранен: {filename}")
                except Exception as e_ss:
                    logging.error(f"[{thread_name}] Не удалось сохранить скриншот при таймауте цены: {e_ss}")
            except NoSuchElementException:
                error_message = f"Элемент АКТУАЛЬНОЙ ЦЕНЫ ({MAIN_PRICE_SELECTOR}) не найден в DOM."
                logging.error(f"[{thread_name}] {error_message}")
            # ИСПРАВЛЕНО: Ловим OSError для ConnectionRefusedError
            except (WebDriverException, MaxRetryError, NewConnectionError, OSError, NoSuchWindowException) as e_interact:
                 error_message = f"Ошибка взаимодействия (поиск цены): {e_interact.__class__.__name__}"
                 logging.error(f"[{thread_name}] {error_message}: {str(e_interact)[:200]}...", exc_info=False)
            except Exception as e_price_main:
                error_message = f"Неожиданная ошибка при поиске/парсинге актуальной цены: {e_price_main}"
                logging.error(f"[{thread_name}] {error_message}", exc_info=True)

        # --- Получаем Название ---
        driver_alive = False
        if driver:
            try:
                driver.current_url; driver_alive = True
            # ИСПРАВЛЕНО: Добавлен OSError, SocketGaiError ловится через него или WebDriverException
            except (WebDriverException, MaxRetryError, NewConnectionError, OSError, NoSuchWindowException) as e_driver_check:
                logging.warning(f"[{thread_name}] Драйвер недоступен перед поиском названия: {e_driver_check.__class__.__name__}")
                if error_message is None: error_message = "Драйвер стал недоступен"

        if driver_alive and (error_message is None or price is not None):
            title_wait_timeout = TITLE_WAIT_TIMEOUT
            try:
                 logging.info(f"[{thread_name}] Ожидание заголовка ({TITLE_SELECTOR}) - до {title_wait_timeout} сек...")
                 title_wait = WebDriverWait(driver, title_wait_timeout)
                 title_tag = title_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, TITLE_SELECTOR)))
                 product_name = title_tag.text.strip()
                 if product_name: logging.info(f"[{thread_name}] Название найдено: {product_name[:50]}...")
                 else: product_name = "Название не найдено (пустой тег)"
            except TimeoutException:
                 logging.warning(f"[{thread_name}] Тег названия ({TITLE_SELECTOR}) не найден за {title_wait_timeout} сек.")
                 if product_name is None: product_name = "Название не найдено (таймаут)"
            # ИСПРАВЛЕНО: Ловим OSError
            except (WebDriverException, MaxRetryError, NewConnectionError, NoSuchWindowException, OSError) as e_title_interact:
                 logging.warning(f"[{thread_name}] Ошибка взаимодействия (поиск названия): {e_title_interact.__class__.__name__}")
                 if product_name is None: product_name = "Название не найдено (ошибка драйвера)"
            except Exception as e_title:
                 logging.warning(f"[{thread_name}] Ошибка получения названия: {e_title}", exc_info=True)
                 if product_name is None: product_name = f"Название не найдено (ошибка)"
        elif product_name is None:
             product_name = "Название не получено (ошибка)"

        # Финальная проверка
        if price is None and error_message is None:
            error_message = "Актуальная цена не была извлечена (неизвестная причина)."
            logging.warning(f"[{thread_name}] {error_message}")

    # ИСПРАВЛЕНО: Добавлен OSError, SocketGaiError
    except (WebDriverException, ProtocolError, ConnectionResetError, MaxRetryError, NewConnectionError, OSError, SocketGaiError) as e_wd:
        error_message = f"Критическая ошибка WebDriver/Connection: {e_wd.__class__.__name__}"
        logging.error(f"[{thread_name}] {error_message}: {str(e_wd)[:200]}...", exc_info=False)
        if product_name is None: product_name = "Название не получено (ошибка драйвера/сети)"
    except Exception as e_main:
        error_message = f"Общая неперехваченная ошибка: {e_main}"
        logging.error(f"[{thread_name}] {error_message}", exc_info=True)
        if product_name is None: product_name = "Название не получено (общая ошибка)"
    finally:
        if driver:
            logging.info(f"[{thread_name}] Закрытие драйвера для {url_snippet}...")
            try:
                if driver.window_handles: driver.quit()
                else: logging.warning(f"[{thread_name}] Окно драйвера уже было закрыто перед quit.")
            except (WebDriverException, OSError) as e_quit:
                 logging.error(f"[{thread_name}] Ошибка при driver.quit(): {e_quit.__class__.__name__}", exc_info=False)
            except Exception as e_quit_other:
                 logging.error(f"[{thread_name}] Неожиданная ошибка при driver.quit(): {e_quit_other}", exc_info=True)
            logging.info(f"[{thread_name}] Драйвер закрыт (или попытка закрытия выполнена).")

    return url, price, product_name, error_message


# --- Функция-обертка для вызова в потоке ---
def check_single_url(url):
    """Обертка для вызова get_price_from_dns в ThreadPoolExecutor."""
    thread_name = threading.current_thread().name
    logging.info(f"[{thread_name}] Начинаю проверку URL: {url[:60]}...")
    try:
        result = get_price_from_dns(url)
        price_log = result[1] if result[1] is not None else "N/A"
        name_log = result[2] if result[2] else "N/A"
        error_log = result[3] if result[3] else "None"
        if error_log != "None" and len(error_log) > 100: error_log = error_log[:100] + "..."
        logging.info(f"[{thread_name}] Проверка URL {url[:60]} завершена. Рез: P={price_log}, N='{name_log[:30]}...', E='{error_log}'")
        return result
    except Exception as e:
        logging.error(f"[{thread_name}] Неперехваченная ошибка в check_single_url для {url[:60]}: {e}", exc_info=True)
        return url, None, None, f"Критическая ошибка в потоке check_single_url: {e}"

# --- Фоновый ПОТОК обновления (С ПУЛОМ ПОТОКОВ) ---
def update_price_periodically():
    global app_state
    current_thread = threading.current_thread()
    logging.info(f"Запуск потока обновления цен [{current_thread.name}] с интервалом {CHECK_INTERVAL_SECONDS} сек и {MAX_WORKERS} воркерами.")
    iteration_count = 0

    while True:
        iteration_count += 1
        start_time_iter = time.time()
        logging.info(f"--- Начало итерации {iteration_count} ---")
        urls_to_process = []
        registered_chat_ids_copy = set()

        try:
            with app_state["lock"]:
                urls_to_process = list(app_state["urls_to_monitor"])
                registered_chat_ids_copy = set(app_state.get("telegram_users", set()))

            if not urls_to_process:
                logging.info("Список URL для мониторинга пуст. Пауза...")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue
            else:
                logging.info(f"Начинается проверка {len(urls_to_process)} URL с использованием {MAX_WORKERS} потоков...")
                results = {}
                history_changed_overall = False
                futures = []

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="PriceCheckWorker") as executor:
                    futures = [executor.submit(check_single_url, url) for url in urls_to_process]

                    processed_count = 0
                    for future in concurrent.futures.as_completed(futures):
                        processed_count += 1
                        try:
                            url_res, price_res, name_res, error_res = future.result()
                            results[url_res] = (price_res, name_res, error_res)
                        except Exception as e_future:
                            logging.error(f"Ошибка при получении результата из future ({processed_count}/{len(urls_to_process)}): {e_future}", exc_info=True)

                processing_start_time = time.time()
                logging.info(f"Все {len(results)} задач проверки завершены. Время выполнения парсинга: {processing_start_time - start_time_iter:.2f} сек. Обработка результатов...")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                notifications_to_send = []

                with app_state["lock"]:
                    for url, (new_price, fetched_name, error) in results.items():
                        if url not in app_state["urls_to_monitor"]:
                            logging.info(f"URL {url[:60]} был удален во время проверки, пропускаем результат.")
                            continue

                        url_snippet = url[:60] + "..." if len(url) > 60 else url

                        if url not in app_state["products"]:
                            app_state["products"][url] = {"history": []}
                        product_data = app_state["products"][url]

                        current_price = product_data.get("price", None)
                        current_name = product_data.get("name", None)
                        price_history = product_data.get("history", [])

                        product_data["last_check_time"] = timestamp
                        product_data["error"] = error

                        display_name = url_snippet
                        is_name_valid = lambda name: name and "Не найдено" not in name and "Не удалось" not in name and "ошибки" not in name and "ошибка" not in name

                        if is_name_valid(fetched_name):
                             display_name = fetched_name
                             if current_name != fetched_name:
                                 product_data["name"] = fetched_name
                                 logging.info(f"Название обновлено для {url_snippet}: '{fetched_name[:50]}...'")
                             else:
                                 product_data["name"] = fetched_name
                        elif is_name_valid(current_name):
                             display_name = current_name
                             product_data["name"] = current_name
                        else:
                             product_data["name"] = fetched_name if fetched_name else "Название не получено"

                        if error:
                            error_short = str(error)[:150] + ('...' if len(str(error)) > 150 else '')
                            logging.error(f"Ошибка при проверке {display_name} ({url_snippet}): {error_short}")
                        elif new_price is not None:
                            price_changed = (current_price is None or current_price != new_price)

                            if price_changed:
                                old_price_str = f"{current_price:,.0f} ₽".replace(",", " ") if current_price is not None else "N/A"
                                price_now_formatted = f"{new_price:,.0f}".replace(",", " ")
                                logging.info(f"Цена изменилась для {display_name}: {price_now_formatted} ₽ (Старая: {old_price_str})")

                                product_data["price"] = new_price
                                new_entry = {"timestamp": timestamp, "price": new_price}
                                price_history.append(new_entry)

                                if len(price_history) > MAX_HISTORY_PER_URL:
                                    product_data["history"] = price_history[-MAX_HISTORY_PER_URL:]
                                else:
                                    product_data["history"] = price_history

                                history_changed_overall = True

                                message_text = (
                                    f"🔔 *Изменение цены!*\n\n"
                                    f"Товар: *{display_name}*\n"
                                    f"Новая цена: *{price_now_formatted} ₽*\n"
                                    f"Старая цена: {old_price_str}\n\n"
                                    f"[Ссылка]({url})"
                                )
                                notifications_to_send.append(message_text)

                            elif current_price is not None:
                                price_formatted = f"{current_price:,.0f}".replace(",", " ")
                                logging.info(f"Цена для {display_name} не изменилась ({price_formatted} ₽)")

                        else:
                             if not error:
                                 logging.warning(f"Нет цены и нет ошибки для {display_name} ({url_snippet}). Проверьте логи парсера и селекторы.")
                                 product_data["error"] = "Внутренняя ошибка: нет цены и ошибки"

                processing_end_time = time.time()
                logging.info(f"Обработка результатов завершена. Время: {processing_end_time - processing_start_time:.2f} сек.")

                if notifications_to_send and registered_chat_ids_copy:
                     full_notification_text = "\n\n---\n\n".join(notifications_to_send)
                     try:
                          app_state["message_queue"].put((list(registered_chat_ids_copy), full_notification_text))
                          logging.info(f"Сводное уведомление ({len(notifications_to_send)} изменений) добавлено в очередь для {len(registered_chat_ids_copy)} пользователей.")
                     except Exception as e_queue:
                          logging.error(f"Ошибка добавления сводного уведомления в очередь: {e_queue}")

                if history_changed_overall:
                    save_price_history()

            end_time_iter = time.time()
            logging.info(f"--- Итерация {iteration_count} завершена. Общее время: {end_time_iter - start_time_iter:.2f} сек. ---")

            logging.info(f"Следующая проверка через {CHECK_INTERVAL_SECONDS} секунд.")
            time.sleep(CHECK_INTERVAL_SECONDS)

        except Exception as e_main_loop:
            logging.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА в главном цикле потока обновления (Итерация {iteration_count}): {e_main_loop}", exc_info=True)
            logging.info("Пауза 60 секунд перед следующей попыткой...")
            time.sleep(60)


# --- Функции для Telegram Бота ---
# ... (Код без изменений) ...
def registered_user_only(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        global app_state
        if not update or not update.effective_chat:
             logging.warning(f"Вызов {func.__name__} без update или effective_chat.")
             return ConversationHandler.END
        chat_id = update.effective_chat.id
        with app_state["lock"]:
            registered_users = app_state.get("telegram_users", set())
        if chat_id not in registered_users:
            logging.warning(f"Неавторизованный доступ от chat_id {chat_id} к функции {func.__name__}")
            reply_target = update.message or update.callback_query.message if update.callback_query else None
            if reply_target:
                 await reply_target.reply_text("Доступ ограничен. Пожалуйста, сначала используйте /start для регистрации.")
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)
    return wrapped

main_keyboard = [
    [KeyboardButton("📊 Статус"), KeyboardButton("📋 Список URL")],
    [KeyboardButton("➕ Добавить URL"), KeyboardButton("➖ Удалить URL")],
    [KeyboardButton("🛑 Отписаться")]
]
main_markup = ReplyKeyboardMarkup(main_keyboard, resize_keyboard=True)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global app_state
    user = update.effective_user
    chat_id = update.effective_chat.id
    logging.info(f"Получена команда /start от user: {user.username} (ID: {chat_id})")
    reply_text = ""
    added_new = False
    send_markup = False

    with app_state["lock"]:
        current_users = app_state.get("telegram_users", set())
        if chat_id in current_users:
            reply_text = f"С возвращением, {user.mention_markdown()}! Бот уже активен для вас." # Markdown V1
            send_markup = True
        elif len(current_users) < MAX_TELEGRAM_USERS:
            current_users.add(chat_id)
            app_state["telegram_users"] = current_users
            added_new = True
            logging.info(f"Новый пользователь добавлен: chat_id {chat_id}. Всего пользователей: {len(current_users)}")
            reply_text = f"Привет, {user.mention_markdown()}! 👋\nВы успешно подписались на уведомления об изменении цен." # Markdown V1
            send_markup = True
        else:
            logging.warning(f"Попытка /start от chat_id {chat_id}, но достигнут лимит пользователей ({MAX_TELEGRAM_USERS}).")
            reply_text = f"Извините, достигнут максимальный лимит пользователей ({MAX_TELEGRAM_USERS}). Новые подписки временно невозможны."
            send_markup = False

    if added_new:
        save_telegram_users()

    reply_args = {"text": reply_text, "parse_mode": ParseMode.MARKDOWN} # V1
    if send_markup:
        reply_args["reply_markup"] = main_markup
    await update.message.reply_text(**reply_args) # Используем reply_text

@registered_user_only
async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global app_state
    user = update.effective_user
    chat_id = update.effective_chat.id
    logging.info(f"Получена команда /stop или 'Отписаться' от user: {user.username} (ID: {chat_id})")
    reply_text = ""
    removed = False

    with app_state["lock"]:
        current_users = app_state.get("telegram_users", set())
        if chat_id in current_users:
            current_users.remove(chat_id)
            app_state["telegram_users"] = current_users
            removed = True
            logging.info(f"Пользователь удален: chat_id {chat_id}. Осталось пользователей: {len(current_users)}")
            reply_text = "Вы успешно отписались от уведомлений. Клавиатура удалена."
        else:
            logging.warning(f"Попытка /stop от chat_id {chat_id}, который не был подписан.")
            reply_text = "Вы не были подписаны на уведомления."

    if removed:
        save_telegram_users()
        await update.message.reply_text(reply_text, reply_markup=telegram.ReplyKeyboardRemove())
    else:
        await update.message.reply_text(reply_text, reply_markup=main_markup)

@registered_user_only
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global app_state
    chat_id = update.effective_chat.id
    logging.info(f"Запрос статуса ('/status' или '📊 Статус') от chat_id {chat_id}")
    base_text = "*📊 Текущий статус отслеживаемых товаров:*\n"
    try:
         await context.bot.send_message(chat_id=chat_id, text=base_text, parse_mode=ParseMode.MARKDOWN_V2)
    except telegram.error.BadRequest as e:
         logging.error(f"Ошибка отправки заголовка статуса: {e}")
         await context.bot.send_message(chat_id=chat_id, text="Статус отслеживаемых товаров:") # Fallback

    message_parts = []
    current_message = ""
    MAX_MESSAGE_LEN = 4000

    with app_state["lock"]:
        urls_to_show = list(app_state.get("urls_to_monitor", []))
        products_data = dict(app_state.get("products", {}))

    if not urls_to_show:
        await context.bot.send_message(chat_id=chat_id, text="Список отслеживаемых URL пуст\\. Добавьте URL\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    for i, url in enumerate(urls_to_show, 1):
        data = products_data.get(url)
        url_snippet = url[:35] + "..." if len(url) > 35 else url
        escaped_url = telegram.helpers.escape_markdown(url, version=2)

        product_block_parts = []

        if not data:
            escaped_url_snippet = telegram.helpers.escape_markdown(url_snippet, version=2)
            product_block_parts.append(f"\n{i}\\. [{escaped_url_snippet}]({escaped_url})\n   ⚠️ Данные еще не загружены\\.")
        else:
            name = data.get('name', "Название не загружено")
            price = data.get('price')
            error = data.get('error')
            last_check = data.get('last_check_time', 'Нет данных')

            escaped_name = telegram.helpers.escape_markdown(name, version=2)
            escaped_last_check = telegram.helpers.escape_markdown(last_check, version=2)

            product_block_parts.append(f"\n{i}\\. *{escaped_name}* \\([ссылка]({escaped_url})\\)")
            if price is not None:
                price_formatted = f"{price:,.0f}".replace(",", " ")
                escaped_price = telegram.helpers.escape_markdown(price_formatted, version=2)
                product_block_parts.append(f"   ✅ *Цена:* {escaped_price} ₽")
            elif error:
                error_str = str(error)
                escaped_error = telegram.helpers.escape_markdown(error_str[:150] + ('\\.\\.\\.' if len(error_str) > 150 else ''), version=2)
                product_block_parts.append(f"   ❌ *Ошибка:* _{escaped_error}_")
            else:
                product_block_parts.append("   ⏳ Цена: _ожидается проверка_")
            product_block_parts.append(f"   🕒 _Проверка:_ {escaped_last_check}")

        product_block_text = "\n".join(product_block_parts)

        if len(current_message) + len(product_block_text) + 2 > MAX_MESSAGE_LEN:
             message_parts.append(current_message)
             current_message = product_block_text.lstrip('\n')
        else:
             if current_message: current_message += "\n" + product_block_text
             else: current_message = product_block_text.lstrip('\n')

    if current_message: message_parts.append(current_message)

    for part in message_parts:
         if not part: continue
         try:
             await context.bot.send_message(
                 chat_id=chat_id, text=part, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
             )
             await asyncio.sleep(0.2)
         except telegram.error.BadRequest as e:
             logging.error(f"Ошибка отправки части статуса (MarkdownV2) в чат {chat_id}: {e}. Часть: {part[:100]}...")
             try:
                 plain_text = telegram.helpers.escape_markdown(part, version=1)
                 await context.bot.send_message(
                     chat_id=chat_id, text="Ошибка форматирования (V2)\\. Показываю как Markdown V1:\n" + plain_text,
                     parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
                 )
             except telegram.error.BadRequest as e_v1:
                  logging.error(f"Не удалось отправить часть статуса даже как Markdown V1: {e_v1}")
                  await context.bot.send_message(chat_id=chat_id, text="Произошла ошибка при отправке части статуса \\(простой текст\\)\\.")
             except Exception as e_fallback:
                  logging.error(f"Неизвестная ошибка при fallback отправке статуса: {e_fallback}")
                  await context.bot.send_message(chat_id=chat_id, text="Критическая ошибка при отправке части статуса\\.")


@registered_user_only
async def list_urls_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global app_state
    chat_id = update.effective_chat.id
    logging.info(f"Запрос списка URL ('/list' или '📋 Список URL') от chat_id {chat_id}")
    base_text = "*📋 Список отслеживаемых URL:*\n"
    try:
        await context.bot.send_message(chat_id=chat_id, text=base_text, parse_mode=ParseMode.MARKDOWN_V2)
    except telegram.error.BadRequest as e:
        logging.error(f"Ошибка отправки заголовка списка URL: {e}")
        await context.bot.send_message(chat_id=chat_id, text="Список отслеживаемых URL:")

    message_parts = []
    current_message = ""
    MAX_MESSAGE_LEN = 4000

    with app_state["lock"]:
        urls = list(app_state.get("urls_to_monitor", []))

    if not urls:
        await context.bot.send_message(chat_id=chat_id, text="Список пуст\\. Добавьте URL\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    for i, url in enumerate(urls, 1):
        escaped_url = telegram.helpers.escape_markdown(url, entity_type=telegram.constants.MessageEntityType.CODE)
        url_line = f"\n{i}\\. `{escaped_url}`"

        if len(current_message) + len(url_line) > MAX_MESSAGE_LEN:
            message_parts.append(current_message)
            current_message = url_line.lstrip('\n')
        else:
            current_message += url_line

    if current_message:
        message_parts.append(current_message)

    for part in message_parts:
        if not part: continue
        try:
            await context.bot.send_message(
                chat_id=chat_id, text=part, parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
            )
            await asyncio.sleep(0.2)
        except telegram.error.BadRequest as e:
            logging.error(f"Ошибка отправки части списка URL (MarkdownV2) в чат {chat_id}: {e}. Часть: {part[:100]}...")
            try:
                plain_text = telegram.helpers.escape_markdown(part, version=1)
                await context.bot.send_message(
                    chat_id=chat_id, text="Ошибка форматирования (V2)\\. Показываю как Markdown V1:\n" + plain_text,
                    parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True
                )
            except telegram.error.BadRequest as e_v1:
                 logging.error(f"Не удалось отправить часть списка URL даже как Markdown V1: {e_v1}")
                 await context.bot.send_message(chat_id=chat_id, text="Произошла ошибка при отправке части списка URL \\(простой текст\\)\\.")
            except Exception as e_fallback:
                 logging.error(f"Неизвестная ошибка при fallback отправке списка URL: {e_fallback}")
                 await context.bot.send_message(chat_id=chat_id, text="Критическая ошибка при отправке части списка URL\\.")


# --- ConversationHandler для добавления URL ---
@registered_user_only
async def add_url_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (без изменений) ...
    chat_id = update.effective_chat.id
    logging.info(f"Начало добавления URL для chat_id {chat_id}")
    await update.message.reply_text(
        "Пришлите один или несколько URL товаров с `dns-shop.ru` (каждый на новой строке).\n"
        "Пример:\n"
        "`https://www.dns-shop.ru/product/xxx/yyy/`\n"
        "`https://www.dns-shop.ru/product/zzz/vvv/`\n\n"
        "Для отмены введите /cancel или нажмите кнопку '❌ Отмена'.",
        parse_mode=ParseMode.MARKDOWN, # V1
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Отмена")]], resize_keyboard=True, one_time_keyboard=True)
    )
    return STATE_WAITING_URL_TO_ADD

async def add_url_received(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (без изменений) ...
    global app_state
    chat_id = update.effective_chat.id
    user_text = update.message.text
    logging.info(f"Получен текст для добавления URL от chat_id {chat_id}")

    new_urls_added_count = 0
    invalid_urls = []
    duplicate_urls = []
    valid_new_urls = []

    if user_text:
        potential_urls = [u.strip() for u in user_text.splitlines() if u.strip()]
        if not potential_urls:
             await update.message.reply_text("Вы не прислали URL. Попробуйте снова или нажмите '❌ Отмена'.", reply_markup=main_markup)
             return ConversationHandler.END

        with app_state["lock"]:
            current_urls_set = set(app_state["urls_to_monitor"])
            for u in potential_urls:
                parsed = urlparse(u)
                if (parsed.scheme in ['http', 'https'] and
                        'dns-shop.ru' in parsed.netloc and
                        '/product/' in parsed.path):
                    if u in current_urls_set:
                        duplicate_urls.append(u)
                    elif u not in valid_new_urls:
                        valid_new_urls.append(u)
                else:
                    invalid_urls.append(u)

            if valid_new_urls:
                app_state["urls_to_monitor"].extend(valid_new_urls)
                for url in valid_new_urls:
                    if url not in app_state["products"]:
                        app_state["products"][url] = {"history": []}
                new_urls_added_count = len(valid_new_urls)
                logging.info(f"Добавлено {new_urls_added_count} новых URL от chat_id {chat_id}.")

        if new_urls_added_count > 0:
            save_urls()

        reply_parts = []
        if new_urls_added_count > 0:
            reply_parts.append(f"✅ Успешно добавлено {new_urls_added_count} новых URL.")
        if duplicate_urls:
            reply_parts.append(f"☑️ Пропущено {len(duplicate_urls)} URL (уже отслеживаются).")
        if invalid_urls:
            reply_parts.append(f"❌ Пропущено {len(invalid_urls)} URL (некорректный формат/не DNS товар).")

        if not reply_parts:
             if potential_urls:
                 reply_parts.append("Не найдено подходящих URL для добавления.")
             else:
                 reply_parts.append("Не получено URL для добавления.")

        await update.message.reply_text("\n".join(reply_parts), reply_markup=main_markup)

    else:
        await update.message.reply_text("Получено пустое сообщение. Отправьте URL или /cancel.", reply_markup=main_markup)
        return STATE_WAITING_URL_TO_ADD

    return ConversationHandler.END

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # ... (без изменений) ...
    chat_id = update.effective_chat.id
    logging.info(f"Диалог добавления URL отменен пользователем chat_id {chat_id}")
    await update.message.reply_text("Действие отменено.", reply_markup=main_markup)
    return ConversationHandler.END

# --- Логика удаления URL ---
@registered_user_only
async def delete_url_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (без изменений) ...
    global app_state
    chat_id = update.effective_chat.id
    logging.info(f"Начало удаления URL для chat_id {chat_id}")
    keyboard = []

    with app_state["lock"]:
        urls = list(app_state.get("urls_to_monitor", []))
        products_data = app_state.get("products", {})

        if not urls:
            await update.message.reply_text("Список отслеживаемых URL пуст. Нечего удалять.", reply_markup=main_markup)
            return
        else:
            for i, url in enumerate(urls):
                product_info = products_data.get(url, {})
                product_name_disp = product_info.get("name")
                button_text = f"❌ {i+1}. "
                is_name_valid = lambda name: name and "Не найдено" not in name and "Не удалось" not in name and "ошибки" not in name

                if is_name_valid(product_name_disp):
                    button_text += product_name_disp[:40] + ("..." if len(product_name_disp) > 40 else "")
                else:
                    try:
                        path_parts = urlparse(url).path.strip('/').split('/')
                        prod_index = path_parts.index('product')
                        url_display = "/".join(path_parts[prod_index+1:prod_index+3])[:40]
                        button_text += url_display + ("..." if len(url_display) >= 40 else "")
                    except (ValueError, IndexError):
                         button_text += url[-40:] + ("..." if len(url) > 40 else "")

                button = InlineKeyboardButton(button_text, callback_data=f"delete_{i}")
                keyboard.append([button])

    if keyboard:
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("Выберите URL для удаления:", reply_markup=reply_markup)
    else:
        await update.message.reply_text("Список URL пуст.", reply_markup=main_markup)

@registered_user_only
async def delete_url_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (без изменений) ...
    global app_state
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    callback_data = query.data

    if not callback_data or not callback_data.startswith("delete_"):
        logging.warning(f"Получен некорректный callback_data '{callback_data}' от chat_id {chat_id}")
        await context.bot.send_message(chat_id=chat_id, text="Произошла ошибка при обработке вашего выбора.")
        return

    try:
        url_index_to_delete = int(callback_data.split("_")[1])
    except (IndexError, ValueError):
        logging.error(f"Не удалось извлечь индекс из callback_data: {callback_data}")
        await context.bot.send_message(chat_id=chat_id, text="Ошибка: Неверный идентификатор для удаления.")
        return

    url_to_delete = None
    url_found_and_removed = False
    remaining_urls_count = 0

    with app_state["lock"]:
        current_urls = app_state.get("urls_to_monitor", [])
        if 0 <= url_index_to_delete < len(current_urls):
            url_to_delete = current_urls.pop(url_index_to_delete)
            url_found_and_removed = True
            app_state["urls_to_monitor"] = current_urls
            remaining_urls_count = len(current_urls)
            logging.info(f"URL #{url_index_to_delete+1} ('{url_to_delete[:60]}...') удален по запросу chat_id {chat_id}.")

            if url_to_delete in app_state["products"]:
                del app_state["products"][url_to_delete]
                logging.info(f"Данные и история для удаленного URL '{url_to_delete[:60]}...' очищены.")
            else:
                 logging.warning(f"Данные продукта для удаляемого URL '{url_to_delete[:60]}...' не найдены.")
        else:
            logging.warning(f"Попытка удалить URL с неверным индексом {url_index_to_delete} от chat_id {chat_id}. Длина списка: {len(current_urls)}")

    if url_found_and_removed:
        save_urls()
        save_price_history()
        msg_text = f"✅ URL '{url_to_delete[:60]}...' успешно удален."
        try:
            await query.edit_message_text(text=msg_text, reply_markup=None)
        except telegram.error.BadRequest as e:
             if "message is not modified" in str(e).lower():
                 logging.warning("Сообщение не изменено при редактировании после удаления URL.")
                 await context.bot.send_message(chat_id=chat_id, text=msg_text, reply_markup=main_markup)
             else:
                 logging.error(f"Ошибка при редактировании сообщения после удаления URL: {e}")
                 await context.bot.send_message(chat_id=chat_id, text=msg_text, reply_markup=main_markup)
        await context.bot.send_message(chat_id=chat_id, text=f"Осталось URL для отслеживания: {remaining_urls_count}.", reply_markup=main_markup)
    else:
        try:
             await query.edit_message_text(text="Не удалось удалить URL (возможно, он уже был удален или список изменился). Попробуйте снова /delete.", reply_markup=None)
        except telegram.error.BadRequest as e:
             logging.error(f"Ошибка при редактировании сообщения об ошибке удаления: {e}")
             await context.bot.send_message(chat_id=chat_id, text="Не удалось удалить URL (возможно, он уже был удален). Попробуйте снова /delete.", reply_markup=main_markup)

# Обработчик неизвестного текста
@registered_user_only
async def unknown_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (без изменений) ...
    await update.message.reply_text(
        "Извините, я не понимаю эту команду. Пожалуйста, используйте кнопки ниже или доступные команды (/start, /status, /list, /add, /delete, /stop).",
        reply_markup=main_markup
    )

# --- Маршруты Flask ---
# ... (Без изменений) ...
@app.route('/')
def index():
    global app_state
    logging.debug("Flask: Запрос к /")
    products_data_copy = {}
    urls_in_state = []

    with app_state["lock"]:
        urls_in_state = list(app_state.get("urls_to_monitor", []))
        all_products = app_state.get("products", {})
        for url in urls_in_state:
            if url in all_products:
                 data = all_products[url]
                 products_data_copy[url] = {
                    "price": data.get("price"),
                    "name": data.get("name"),
                    "history": list(data.get("history", [])),
                    "last_check_time": data.get("last_check_time"),
                    "error": data.get("error")
                }
            else:
                 products_data_copy[url] = {"name": "Данные загружаются...", "history": []}

    logging.debug(f"Flask: Данные для {len(products_data_copy)} URL подготовлены для рендеринга.")
    refresh_interval_page = CHECK_INTERVAL_SECONDS + 15
    refresh_interval_page = max(60, refresh_interval_page)

    return render_template(
        'index.html',
        products_data=products_data_copy,
        refresh_interval=refresh_interval_page
    )

@app.route('/add_urls', methods=['POST'])
def add_urls():
    global app_state
    if request.method == 'POST':
        urls_text = request.form.get('urls', '')
        new_urls_added_count = 0
        valid_new_urls = []
        invalid_urls = []
        duplicate_urls = []
        logging.info(f"Flask: Получен POST запрос на /add_urls")

        if urls_text:
            potential_urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
            with app_state["lock"]:
                current_urls_set = set(app_state["urls_to_monitor"])
                for u in potential_urls:
                    parsed = urlparse(u)
                    if (parsed.scheme in ['http', 'https'] and
                            'dns-shop.ru' in parsed.netloc and
                            '/product/' in parsed.path):
                        if u in current_urls_set:
                            duplicate_urls.append(u)
                        elif u not in valid_new_urls:
                             valid_new_urls.append(u)
                    else:
                        invalid_urls.append(u)

                if valid_new_urls:
                    app_state["urls_to_monitor"].extend(valid_new_urls)
                    for url in valid_new_urls:
                        if url not in app_state["products"]:
                            app_state["products"][url] = {"history": []}
                    new_urls_added_count = len(valid_new_urls)

            if new_urls_added_count > 0:
                logging.info(f"Flask: Добавлено {new_urls_added_count} новых URL через веб-интерфейс.")
                save_urls()
            else:
                 logging.info("Flask: Новые валидные URL не добавлены через веб-интерфейс.")

            if invalid_urls:
                 logging.warning(f"Flask: Обнаружены невалидные URL: {len(invalid_urls)}")
            if duplicate_urls:
                 logging.info(f"Flask: Обнаружены дубликаты URL: {len(duplicate_urls)}")
        else:
            logging.info("Flask: Пустой запрос на добавление URL.")

    return redirect(url_for('index'))

@app.route('/delete_url', methods=['POST'])
def delete_url():
    global app_state
    url_to_delete = request.form.get('url_to_delete')
    if not url_to_delete:
        logging.warning("Flask: Запрос на удаление без URL.")
        return redirect(url_for('index'))

    logging.info(f"Flask: Получен POST запрос на /delete_url для: {url_to_delete[:60]}...")
    url_found_and_removed = False

    with app_state["lock"]:
        if url_to_delete in app_state["urls_to_monitor"]:
            try:
                app_state["urls_to_monitor"].remove(url_to_delete)
                url_found_and_removed = True
                logging.info(f"Flask: URL '{url_to_delete[:60]}...' удален из списка мониторинга.")
                if url_to_delete in app_state["products"]:
                    del app_state["products"][url_to_delete]
                    logging.info(f"Flask: Данные продукта для '{url_to_delete[:60]}...' удалены.")
            except ValueError:
                 logging.warning(f"Flask: Ошибка при удалении URL '{url_to_delete[:60]}...' (не найден при remove).")
                 url_found_and_removed = False
        else:
            logging.warning(f"Flask: URL '{url_to_delete[:60]}...' для удаления не найден в списке мониторинга.")

    if url_found_and_removed:
        save_urls()
        save_price_history()
    else:
        logging.warning("Flask: URL не был удален (либо не найден, либо ошибка).")

    return redirect(url_for('index'))

# --- Функция-обертка для запуска Telegram бота в отдельном потоке ---
# ... (Без изменений) ...
def run_telegram_processing(app_instance: Application, message_queue: queue.Queue):
    bot_instance = app_instance.bot
    loop = None

    async def process_queue():
        logging.info("Запуск обработчика очереди Telegram сообщений...")
        while True:
            try:
                try:
                    chat_ids, message_text = message_queue.get(timeout=1.0)
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue

                logging.info(f"Обработка сообщения из очереди для {len(chat_ids)} пользователей.")
                tasks = []
                send_count = 0
                fail_count = 0
                for chat_id in chat_ids:
                    try:
                        tasks.append(
                            bot_instance.send_message(
                                chat_id=chat_id,
                                text=message_text,
                                parse_mode=ParseMode.MARKDOWN, # V1 для уведомлений
                                disable_web_page_preview=True
                            )
                        )
                        await asyncio.sleep(0.05)
                    except Exception as e_create_task:
                         logging.error(f"Ошибка при создании задачи отправки в чат {chat_id}: {e_create_task}")
                         fail_count += 1

                if tasks:
                     logging.info(f"Запуск {len(tasks)} задач на отправку сообщений...")
                     results = await asyncio.gather(*tasks, return_exceptions=True)
                     for i, result in enumerate(results):
                         target_chat_id = chat_ids[i] if i < len(chat_ids) else "UNKNOWN_CHAT_ID"
                         if isinstance(result, Exception):
                             fail_count += 1
                             if isinstance(result, telegram.error.BadRequest):
                                 logging.warning(f"Ошибка BadRequest при отправке в чат {target_chat_id}: {result}")
                             elif isinstance(result, telegram.error.Forbidden):
                                 logging.warning(f"Ошибка Forbidden (бот заблокирован?) при отправке в чат {target_chat_id}: {result}")
                             else:
                                 logging.error(f"Неожиданная ошибка при отправке сообщения в чат {target_chat_id}: {result}", exc_info=isinstance(result, Exception))
                         else:
                             send_count += 1
                     logging.info(f"Отправка из очереди завершена. Успешно: {send_count}, Ошибки: {fail_count}")
                else:
                     logging.info("Нет задач для отправки из очереди.")
                message_queue.task_done()
            except asyncio.CancelledError:
                 logging.info("Обработчик очереди сообщений остановлен (CancelledError).")
                 break
            except Exception as e_queue_process:
                logging.error(f"Ошибка в цикле обработки очереди сообщений: {e_queue_process}", exc_info=True)
                await asyncio.sleep(5)

    async def run_polling_and_queue():
        try:
            await app_instance.initialize()
            logging.info("Telegram Application инициализировано.")
            await app_instance.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            logging.info("Telegram Updater (polling) запущен.")
            await app_instance.start()
            logging.info("Telegram Application запущено.")
            await process_queue()
        except Exception as e_run_init:
            logging.error(f"Критическая ошибка при запуске Telegram бота или обработчика очереди: {e_run_init}", exc_info=True)
        finally:
            logging.info("Начало остановки компонентов Telegram...")
            if app_instance.running:
                 logging.info("Остановка Telegram Application...")
                 await app_instance.stop()
                 logging.info("Telegram Application остановлено.")
            if app_instance.updater and app_instance.updater.running:
                 logging.info("Остановка Telegram Updater (polling)...")
                 await app_instance.updater.stop()
                 logging.info("Telegram Updater (polling) остановлен.")
            logging.info("Завершение работы Telegram Application (shutdown)...")
            await app_instance.shutdown()
            logging.info("Telegram Application завершило работу (shutdown).")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        logging.info(f"Новый asyncio event loop создан и установлен для потока {threading.current_thread().name}")
        loop.run_until_complete(run_polling_and_queue())
    except Exception as e_loop_runner:
        logging.error(f"Ошибка при выполнении asyncio loop в потоке Telegram: {e_loop_runner}", exc_info=True)
    finally:
        if loop:
            try:
                 if loop.is_running():
                     logging.info(f"Остановка задач и asyncio loop в потоке {threading.current_thread().name}...")
                     loop.stop()
                 logging.info(f"Закрытие asyncio loop в потоке {threading.current_thread().name}...")
                 if not loop.is_closed():
                      loop.close()
                      logging.info(f"Asyncio loop успешно закрыт.")
                 else:
                      logging.info(f"Asyncio loop уже был закрыт.")
            except RuntimeError as e_runtime:
                logging.error(f"RuntimeError при остановке/закрытии asyncio loop: {e_runtime}", exc_info=True)
            except Exception as e_loop_close:
                logging.error(f"Неожиданная ошибка при остановке или закрытии asyncio loop: {e_loop_close}", exc_info=True)
        asyncio.set_event_loop(None)
        logging.info(f"Asyncio event loop сброшен для потока {threading.current_thread().name}.")


# --- Запуск основного приложения ---
if __name__ == '__main__':
    print("--- Запуск DNS Price Monitor ---")

    # Предварительный патчинг undetected-chromedriver
    try:
        print("Предварительная подготовка (патчинг) undetected-chromedriver...")
        patcher = uc.Patcher()
        patcher.auto()
        print(f"Предварительный патчинг chromedriver завершен. Используется: {patcher.executable_path}")
    except Exception as e_patch:
        logging.error(f"!!! ОШИБКА во время предварительного патчинга undetected-chromedriver: {e_patch}", exc_info=True)
        print(f"!!! НЕ УДАЛОСЬ подготовить chromedriver. Ошибка: {e_patch}")
        print("!!! Выход из приложения.")
        exit(1)

    logging.info("Инициализация приложения...")
    load_urls()
    load_price_history()
    load_telegram_users()
    # load_proxies() # Не используется

    logging.info("Настройка Telegram бота...")
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    with app_state["lock"]:
        app_state["telegram_app"] = application

    # --- Регистрация обработчиков ---
    add_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_url_start),
            MessageHandler(filters.Text("➕ Добавить URL"), add_url_start)
        ],
        states={
            STATE_WAITING_URL_TO_ADD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_url_received)]
        },
        fallbacks=[
            CommandHandler("cancel", cancel_command),
            MessageHandler(filters.Text(["❌ Отмена", "Отмена", "cancel"]), cancel_command)
        ],
    )
    application.add_handler(add_conv_handler)

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(MessageHandler(filters.Text("📊 Статус"), status_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(MessageHandler(filters.Text("🛑 Отписаться"), stop_command))
    application.add_handler(CommandHandler("list", list_urls_command))
    application.add_handler(MessageHandler(filters.Text("📋 Список URL"), list_urls_command))
    application.add_handler(CommandHandler("delete", delete_url_start))
    application.add_handler(MessageHandler(filters.Text("➖ Удалить URL"), delete_url_start))
    application.add_handler(CallbackQueryHandler(delete_url_button, pattern="^delete_"))

    # Обработчик неизвестного текста
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_text))

    logging.info("Обработчики Telegram зарегистрированы.")

    # --- Запуск фоновых потоков ---
    price_update_thread = threading.Thread(
        target=update_price_periodically,
        name="PriceUpdaterThread",
        daemon=True
    )
    price_update_thread.start()
    logging.info("Поток обновления цен запущен.")

    telegram_process_thread = threading.Thread(
        target=run_telegram_processing,
        args=(application, app_state["message_queue"]),
        name="TelegramProcessThread",
        daemon=True
    )
    telegram_process_thread.start()
    logging.info("Поток обработки Telegram (polling и очередь) запущен.")

    # --- Запуск веб-сервера Flask ---
    logging.info("Запуск веб-сервера Flask...")
    try:
        from waitress import serve
        print(f"Веб-интерфейс доступен по адресу http://0.0.0.0:5000")
        serve(app, host='0.0.0.0', port=5000, threads=4)
    except ImportError:
        logging.warning("Модуль 'waitress' не найден. Запуск с использованием встроенного сервера разработки Flask.")
        print("ВНИМАНИЕ: Запущен сервер разработки Flask. Для продакшена установите waitress: pip install waitress")
        print(f"Веб-интерфейс доступен по адресу http://0.0.0.0:5000")
        try:
            app.run(host='0.0.0.0', port=5000, debug=False)
        except OSError as e_os:
            if "запрашиваемый адрес для своего контекста" in str(e_os) or "address already in use" in str(e_os).lower():
                logging.error(f"!!! ОШИБКА: Порт 5000 уже занят. Ошибка: {e_os}")
                print(f"!!! Порт 5000 уже используется.")
            else:
                logging.error(f"Ошибка OSError при запуске Flask dev server: {e_os}", exc_info=True)
            exit(1)
    except OSError as e_os:
         if "запрашиваемый адрес для своего контекста" in str(e_os) or "address already in use" in str(e_os).lower():
              logging.error(f"!!! ОШИБКА: Порт 5000 уже занят (Waitress). Ошибка: {e_os}")
              print(f"!!! Порт 5000 уже используется.")
              exit(1)
         else:
              logging.error(f"Ошибка OSError при запуске Waitress: {e_os}", exc_info=True)
    except KeyboardInterrupt:
        logging.info("Получен сигнал прерывания (Ctrl+C). Завершение работы...")
    except Exception as e_serve:
        logging.error(f"Непредвиденная ошибка при запуске веб-сервера: {e_serve}", exc_info=True)

    # --- Завершение работы ---
    logging.info("Приложение завершает работу.")
    print("--- Монитор цен остановлен ---")
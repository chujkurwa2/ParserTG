import time
import google.generativeai as genai
import logging
import json
import os

import google.oauth2.service_account
import openai
import pandas as pd
from datetime import datetime, timedelta
import pytz  # ### ИЗМЕНЕНИЕ ### Добавляем библиотеку для работы с часовыми поясами
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler,
    Filters, CallbackContext, ConversationHandler
)
import gspread
from google.oauth2.service_account import Credentials
from difflib import SequenceMatcher
import threading
import schedule
from gspread_formatting import CellFormat, Color, format_cell_ranges, \
    set_frozen  # ### ИЗМЕНЕНИЕ ### Импортируем batch_format

# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = '8138310577:AAGm6oKHlSD2KHCWfScMZE1lIUOunpBI-sg'
OPENAI_API_KEY = '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'
GOOGLE_SHEET_ID = '1rrjD_SpB79V0djuW-lDP_hIRptzdrITauRoybuJkoqA'

GOOGLE_SHEET_RAW = 'Raw'
GOOGLE_SHEET_CATALOG = 'Catalog'
CUSTOM_GROUPS_FILE = 'custom_groups.json'

### ИЗМЕНЕНИЕ ### Новые константы для управления статусом цены
PRICE_STATUS_COL = 'Статус цены'
STATUS_CURRENT = 'Актуальная'
STATUS_INCREASED = 'Повышена'

openai.api_key = OPENAI_API_KEY

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file('credentials.json', scopes=scope)
gsheet = gspread.authorize(credentials)

ART_CSV_PATH = 'articules.csv'

# Загружаем CSV с артикулами и создаём словарь key → SKU
if os.path.exists(ART_CSV_PATH):
    art_df = pd.read_csv(ART_CSV_PATH)
else:
    logging.warning(f"Файл {ART_CSV_PATH} не найден.")
    art_df = pd.DataFrame()  # Создаем пустой DataFrame, чтобы избежать ошибок

# === СОСТОЯНИЯ ===
ASK_SUPPLIER = 1

# === ПАМЯТЬ ===
user_messages = {}
pending_products = {}
custom_groups = {}

# === ЛОГГИРОВАНИЕ ===
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


def load_custom_groups():
    if os.path.exists(CUSTOM_GROUPS_FILE):
        with open(CUSTOM_GROUPS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_custom_groups(groups):
    with open(CUSTOM_GROUPS_FILE, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)


custom_groups = load_custom_groups()


def get_device_group(model):
    model_lower = model.lower()
    if "macbook" in model_lower:
        return "MacBook"
    elif "iphone" in model_lower:
        return "iPhone"
    elif "airpods" in model_lower:
        return "AirPods"
    elif "pixel" in model_lower:
        return "Pixel"
    elif "ipad" in model_lower:
        return "iPad"
    elif "dji" in model_lower:
        return "DJI"
    elif "jbl" in model_lower or "charge" in model_lower:
        return "Speaker"
    elif model in custom_groups:
        return custom_groups[model]
    else:
        return None


try:
    genai.configure(api_key="AIzaSyBdLrfG1o3bT-Ldihx4hcwCvs4COOdpfXI")
except KeyError:
    print("Ошибка: Переменная окружения GEMINI_API_KEY не найдена.")
    print("Установите ее перед запуском: export GEMINI_API_KEY='ваш_ключ'")
    exit()
csv_filepath = 'articules.csv'


def download_gsheet_as_csv(spreadsheet_name, sheet_name, credentials_path, output_csv_path):
    """
    Скачивает данные из Google Таблицы, используя современный метод аутентификации,
    и сохраняет их в CSV файл.
    """
    try:
        print("Подключаюсь к Google Sheets...")

        # СОВРЕМЕННЫЙ СПОСОБ АУТЕНТИФИКАЦИИ
        # gspread сам использует google-auth для работы с сервисным аккаунтом
        client = gspread.service_account(filename=credentials_path)

        print(f"Открываю таблицу: '{spreadsheet_name}'...")
        spreadsheet = client.open(spreadsheet_name)

        print(f"Открываю лист: '{sheet_name}'...")
        worksheet = spreadsheet.worksheet(sheet_name)

        print("Получаю все данные с листа...")
        data = worksheet.get_all_records()

        if not data:
            print("Ошибка: Лист пустой или не удалось прочитать данные.")
            return False

        df = pd.DataFrame(data)
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

        print(f"✔ Данные успешно скачаны и сохранены в файл: {output_csv_path}")
        return True

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ Ошибка: Таблица '{spreadsheet_name}' не найдена.")
        print("Проверьте, что: 1. Имя таблицы написано верно. 2. Вы поделились таблицей с email из credentials.json.")
        return False
    except gspread.exceptions.WorksheetNotFound:
        print(
            f"❌ Ошибка: Лист '{sheet_name}' не найден в таблице '{spreadsheet_name}'. Проверьте правильность имени листа (вкладки).")
        return False
    except FileNotFoundError:
        print(f"❌ Ошибка: Не найден файл с ключом доступа '{credentials_path}'.")
        print("Убедитесь, что файл 'credentials.json' находится в той же папке, что и скрипт.")
        return False
    except Exception as e:
        print(f"❌ Произошла непредвиденная ошибка: {e}")
        return False


# --- НАСТРОЙКИ ---
# ▼▼▼ ЗАМЕНИТЕ ЭТИ ДВА ЗНАЧЕНИЯ НА СВОИ ▼▼▼
SPREADSHEET_NAME = "Артикулы"  # <-- Вставьте сюда точное имя вашей таблицы
SHEET_NAME = "Лист1"  # <-- Вставьте сюда точное имя нужного листа
# ▲▲▲ БОЛЬШЕ НИЧЕГО МЕНЯТЬ НЕ НУЖНО ▲▲▲

# Пути к файлам (настроены автоматически)
try:
    # Этот путь будет работать при запуске как скрипта (.py)
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Этот путь будет работать в интерактивных средах типа Jupyter
    script_dir = os.getcwd()

CREDENTIALS_FILE = os.path.join(script_dir, "electronicsparser-09e95e686044.json")
OUTPUT_CSV_FILE = os.path.join(script_dir, "articules.csv")


def normalize_with_ai(messages, supplier):
    print("--- Запуск скрипта обновления данных ---")

    # Шаг 1: Скачиваем актуальные данные
    download_successful = download_gsheet_as_csv(
        spreadsheet_name=SPREADSHEET_NAME,
        sheet_name=SHEET_NAME,
        credentials_path=CREDENTIALS_FILE,
        output_csv_path=OUTPUT_CSV_FILE
    )
    if download_successful:
        print("\n--- Запуск основного парсера ---")
        try:
            # Ваш старый код теперь работает со свежим файлом 'articules.csv'
            df_articules = pd.read_csv(OUTPUT_CSV_FILE)

            print("Файл 'articules.csv' успешно загружен.")
            print("Первые 5 строк данных:")
            print(df_articules.head())

            #
            # ... ЗДЕСЬ НАЧИНАЕТСЯ ВАШ КОД С НЕЙРОСЕТЬЮ ...
            # Например: process_data(df_articules)
            #

        except Exception as e:
            print(f"Ошибка при обработке файла 'articules.csv': {e}")

    """
    Нормализует список товаров с помощью Google Gemini API в режиме JSON.
    """
    text = "".join(messages)
    # Промпт остается практически без изменений, т.к. он очень хорошо составлен.
    # Gemini отлично поймет эту структуру.

    try:
        uploaded_file = genai.upload_file(path=csv_filepath,
                                          display_name="CSV с товарами поставщика")
        print(f"Файл успешно загружен: {uploaded_file.uri}")
    except FileNotFoundError:
        logging.error(f"Файл не найден по пути: {csv_filepath}")
        return []
    except Exception as e:
        logging.error(f"Произошла ошибка при загрузке файла: {e}")
        return []

    prompt = f"""
       Твоя задача — преобразовать список товаров в JSON-массив.
       СТРОГО СЛЕДУЙ ПРАВИЛАМ
       Вывод: Только JSON-массив объектов. Без комментариев, пояснений и markdown-форматирования.
       Структура объекта: 

         "model": "String",
         "memory": "String",
         "color": "String",
         "price": "Number",
         "quantity": "Number",
         "supplier": "{supplier}",
         "datetime": "0000-00-00 00:00"
         "market_sku": "String"

       ПРАВИЛА ОБРАБОТКИ ПОЛЕЙ:
       model:
           Нормализуй имя: "IPhone", "MacBook", "Dyson". Добавляй бренд, если он отсутствует, но очевиден (например, "V11" -> "Dyson V11").
           В конец названия в скобках добавь спецификацию по региону, если она указана (из кода LL/A, ZP/A, RU/A или флага 🇺🇸).
            Всегда дописывай в конце год выпуска устройства в скобках. Если ты никак не можешь понять его, то пиши год выпуска (2222)
           ПРАВИЛА ДЛЯ КОНКРЕТНЫХ УСТРОЙСТВ:

           Телефоны (SIM):
               - США (LL/A, 🇺🇸): (dual esim)
               - Гонконг/Китай (ZP/A, CH/A): (2 nano sim)
               - Европа/Россия/Другие (RU/A, F/A и т.д.): (nano sim + esim)

           MacBook (клавиатура):
               - США (LL/A, 🇺🇸) или Азия (ZP/A, CH/A): Добавь в конец названия (ANSI).
               - Европа (RU/A, F/A, и т.д.) или Россия: Добавь в конец названия (ISO).
               - Пример: MacBook Air M2 LL/A -> MacBook Air M2 (ANSI)
               Если информации нет - ставь (ANSI). Но обязательно должно быть что-то написано.
               + Важный параметр - внешняя и внутренняя память. Записывай её через "/". Пример: "24/512Gb, 12/256Gb".
               + Не пиши количество ядер. 
                
           Apple Watch: Укажи размер и тип ремешка, а так же год выпуска. Поставщики обычно его пишут. Если нету, то найди информацию сам.
           Ipad: Бывают версии LTE и без LTE. Учитывай это. Устройство с LTE отличается от устройства без. + важен год выпуска. Пиши его в конце названия в скобках. 

           ВАЖНО: Не включай в model память, цвет или год. Это отдельные поля.
           Если это не iPhone, зависимость между страной и типом SIM-карт будет отличаться. Тебе нужно самому найти информацию, исходя из страны производителя, которую зачастую пишут поставщики в смайликах флагах или текстом.
       price:
           Конвертируй в число. 71.2 -> 71200. 125,000 -> 125000.
           Если цена отрицательная (-49999), сделай ее положительной (49999).
           Цена не может быть null. Если цену определить невозможно, пропусти товар.
           Поставщикки могут ошибаться в ценах, добавлять лишие или наоборот не писать нули. Сверяй цену с товаром, если цена сильно подозрительная, то не добавляй её.
       quantity:
           Если не указано, ставь 1.
           
        ВАЖНО: Если в товаре указано (AS IS), то игнорируй это устройство. Не добавляй его в ответ. Остальные - добавляй.

       ПРАВИЛА ФИЛЬТРАЦИИ:
       Игнорируй (не добавляй в JSON):
           - Товары с крестиком (❌) или перечеркнутым текстом.
           - Предложения с оптовыми ценами ("от 10 шт.", "опт").
           - Товары без цены.

       Артикулы:
           Тебе даётся файл в формате CSV с артикулами и моделями устройств. Найди в таблице CSV артикул, принадлежащий устройству. В этом CSV файле есть два столбца: Артикул(SKU), Наименование.
           Если артикула нету, оставь поле пустым, но добавь устройство.
           Исходная строка: iPhone 15 Pro Max 256GB ZP/A Natural 115.5

           Пример результата в JSON (объект в массиве):

             "model": "iPhone 15 Pro Max (2 nano sim) (2020)",
             "memory": "256GB",
             "color": "Natural",
             "price": 115500,
             "quantity": 1,
             "supplier": "{supplier}",
             "datetime": "0000-00-00 00:00"
             "market_sku": "d4u-iphone15-pro-max-256-natural"
        Старайся обработать каждый товар! НЕ ПЕРЕПУТАЙ ФАЙЛ С АРТИКУЛАМИ И СООБЩЕНИЯ ОТ ПОСТАВЩИКОВ
        Не дублируй устройства. Один товар тебе написан - один товар в ответе от тебя.
        Даю тебе строгие правила наименования модели:
            У Устройств, у которых есть память мы ВСЕГДА дописываем "Gb" в конце. Пример как это должно выглядеть - MacBook Air 13 M3 (ANSI) 24/512Gb Starlight.
        
       Вот товары для обработки:
       {text}
       """

    try:
        print("[AI] Отправляем в Gemini...")

        # 1. Выбираем модель
        model = genai.GenerativeModel('gemini-2.5-pro')

        # 2. Включаем JSON Mode для гарантированного получения JSON
        generation_config = genai.types.GenerationConfig(
            response_mime_type="application/json",
            temperature=0.1
        )

        # 3. Отправляем запрос
        response = model.generate_content(
            [prompt, uploaded_file],
            generation_config=generation_config
        )

        print("[AI] Ответ получен")
        content = response.text.strip()
        print(f"[AI] CONTENT RAW:{content}")

        if not content:
            raise ValueError("AI вернул пустой ответ")

        return json.loads(content)

    except Exception as e:
        logging.error(f"Google AI Error: {e}")
        return []


import hashlib


def generate_model_id(model: str, memory: str, color: str, country: str) -> str:
    base_string = f"{model}|{memory}|{color}|{country}".lower().strip()
    return hashlib.sha1(base_string.encode('utf-8')).hexdigest()[:10]


# ... (остальные функции get_device_group, load/save_custom_groups и т.д. остаются без изменений) ...

def handle_keyboard_input(update: Update, context: CallbackContext):
    text = (update.message.text or update.message.caption or "").lower()
    if "сформировать" in text:
        return start_form(update, context)
    elif "очистить" in text:
        return clear_messages(update, context)
    elif "таблица" in text:
        return send_table_link(update, context)
    else:
        return handle_message(update, context)


def send_table_link(update: Update, context: CallbackContext):
    url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"
    update.message.reply_text(f"📊 Вот ссылка на таблицу:\n{url}")


### ИЗМЕНЕНИЕ ### Логика обновления таблицы полностью переработана
### ИЗМЕНЕНИЕ ### Логика обновления таблицы полностью переработана для исправления KeyError
def update_google_sheets(products):
    """
    Обрабатывает новые продукты и выполняет умное слияние с листом 'List',
    сохраняя статусы и цены старых, необновленных позиций.
    """
    if not products:
        logging.info("Нет новых продуктов для обработки.")
        return

    # --- ЧАСТЬ 1: Запись новых данных в 'Raw' (для истории) ---
    try:
        raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_RAW)
    except gspread.exceptions.WorksheetNotFound:
        raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).add_worksheet(title=GOOGLE_SHEET_RAW, rows="1000", cols="20")

    header = ['model', 'memory', 'color', 'price', 'quantity', 'supplier', 'datetime', 'country', 'market_sku']
    if not raw_sheet.get_all_values():
        raw_sheet.append_row(header, value_input_option='USER_ENTERED')

    new_raw_rows = []
    moscow_tz = pytz.timezone("Europe/Moscow")
    now_moscow = datetime.now(moscow_tz).strftime("%Y-%m-%d %H:%M:%S")

    for p in products:
        model = p.get('model', '').strip()
        country = 'USA' if '🇺🇸' in model or 'll/a' in model.lower() else 'N/A'
        try:
            price = float(p.get('price') or 0)
            if price <= 0: continue
            new_raw_rows.append([
                model, p.get('memory'), p.get('color'), price, p.get('quantity', 1),
                p.get('supplier'), now_moscow, country, p.get('market_sku', '')
            ])
        except (ValueError, TypeError) as e:
            logging.error(f"[Raw] Ошибка при обработке продукта: {p} -> {e}")

    if new_raw_rows:
        raw_sheet.append_rows(new_raw_rows, value_input_option='USER_ENTERED')
        logging.info(f"[Google Sheets] Добавлено {len(new_raw_rows)} строк в 'Raw'.")

    # --- ЧАСТЬ 2: Подготовка "патча" из только что полученных данных ---
    df_new = pd.DataFrame(new_raw_rows, columns=header)
    df_new['price'] = pd.to_numeric(df_new['price'], errors='coerce')
    df_new['datetime'] = pd.to_datetime(df_new['datetime'])
    df_new.dropna(subset=['price', 'datetime'], inplace=True)

    df_new['ID'] = df_new.apply(lambda row: generate_model_id(
        str(row.get('model', '')), str(row.get('memory', '')), str(row.get('color', '')), str(row.get('country', ''))
    ), axis=1)

    # Агрегируем только новые данные, чтобы найти среди них лучшие цены
    df_new['min_price'] = df_new.groupby('ID')['price'].transform('min')
    cheapest_new_df = df_new[df_new['price'] == df_new['min_price']]
    new_summary_df = cheapest_new_df.groupby('ID').agg({
        'model': 'first', 'memory': 'first', 'color': 'first', 'price': 'min', 'datetime': 'first',
        'country': 'first', 'market_sku': 'first',
        'supplier': lambda s: ', '.join(sorted(s.dropna().unique()))
    }).reset_index()

    # Формируем "патч" с правильными колонками для листа "List"
    def create_display_name(row):
        base = str(row['model']).strip()
        if pd.notna(row['memory']) and str(row['memory']) not in base: base += f" {row['memory']}"
        if pd.notna(row['color']) and str(row['color']) not in base: base += f" {row['color']}"
        if row.get('country') == 'USA': base += " 🇺🇸"
        return base.strip()

    new_summary_df['Модель'] = new_summary_df.apply(create_display_name, axis=1)
    new_summary_df.rename(columns={'price': 'Минимальная цена', 'supplier': 'Поставщик'}, inplace=True)
    new_summary_df['Дата последней записи'] = new_summary_df['datetime'].dt.strftime('%d.%m.%Y')
    new_summary_df['Актуальность'] = 'Да'
    new_summary_df[PRICE_STATUS_COL] = STATUS_CURRENT

    # --- ЧАСТЬ 3: Чтение текущего состояния из 'List' и выполнение слияния ---
    try:
        list_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List")
        existing_values = list_sheet.get_all_values()
        if len(existing_values) > 1:
            df_list = pd.DataFrame(existing_values[1:], columns=existing_values[0])
        else:
            df_list = pd.DataFrame(columns=existing_values[0] if existing_values else None)
    except gspread.exceptions.WorksheetNotFound:
        list_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).add_worksheet(title="List", rows="1000", cols="20")
        df_list = pd.DataFrame()

    # Убедимся, что у обоих DataFrame одинаковые колонки для слияния
    final_columns = ['Модель', 'Минимальная цена', 'Поставщик', 'Дата последней записи',
                     'Актуальность', 'market_sku', 'ID', PRICE_STATUS_COL]

    # Приводим оба датафрейма к единому формату колонок
    df_patch = new_summary_df.reindex(columns=final_columns)
    df_list = df_list.reindex(columns=final_columns)

    # Выполняем слияние: ставим новые данные первыми и удаляем дубликаты по ID,
    # оставляя таким образом свежие данные и сохраняя старые, которых не было в обновлении.
    combined_df = pd.concat([df_patch, df_list])
    final_df = combined_df.drop_duplicates(subset=['ID'], keep='first')
    final_df.sort_values(by='Модель', inplace=True, na_position='last')

    # --- ЧАСТЬ 4: Запись результата обратно в 'List' ---
    list_sheet.clear()
    list_sheet.update([final_df.columns.values.tolist()] + final_df.fillna('').values.tolist(),
                      value_input_option='USER_ENTERED')
    logging.info(f"[Google Sheets] Лист 'List' обновлен. Всего позиций: {len(final_df)}.")

    apply_conditional_formatting(list_sheet)

def handle_message(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    # Добавим поддержку текста из caption
    text = update.message.text or update.message.caption

    logging.info(f"[input] Сообщение от пользователя {uid}: {text[:50]}...")

    if text:
        user_messages.setdefault(uid, []).append(text)
        update.message.reply_text("✅ Сообщение сохранено.")
    else:
        update.message.reply_text("⚠️ Сообщение не содержит текста.")


def start(update: Update, context: CallbackContext):
    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("Сформировать"), KeyboardButton("Очистить")],
         [KeyboardButton("Таблица")]],
        resize_keyboard=True
    )
    update.message.reply_text("Добро пожаловать! Выберите действие:", reply_markup=keyboard)


def button_callback(update: Update, context: CallbackContext):
    update.callback_query.answer()
    update.callback_query.message.reply_text("Введи имя поставщика:")
    return ASK_SUPPLIER


def receive_supplier(update: Update, context: CallbackContext):
    supplier = update.message.text
    uid = update.effective_user.id
    messages = user_messages.get(uid, [])
    messages = [m.strip() for m in messages if m.strip()]
    messages = list(set(messages))

    if not messages:
        update.message.reply_text("Нет сообщений для обработки.")
        return ConversationHandler.END

    logging.info(f"[AI] Обработка {len(messages)} сообщений от поставщика '{supplier}'...")
    update.message.reply_text(
        f"🤖 Начинаю обработку {len(messages)} сообщений от '{supplier}'. Это может занять некоторое время...")

    products = normalize_with_ai(messages, supplier)

    if not products:
        update.message.reply_text("⚠️ Не удалось распознать товары. Проверьте формат сообщений или попробуйте позже.")
        return ConversationHandler.END

    logging.info(f"[AI] Получено {len(products)} товаров от AI.")

    update_google_sheets(products)
    update.message.reply_text(f"✅ Готово! Обработано и обновлено в таблице: {len(products)} товаров.")

    user_messages[uid] = []
    return ConversationHandler.END


# ... (остальные хендлеры ask_next_group, handle_group_decision остаются без изменений) ...

### ИЗМЕНЕНИЕ ### Старая функция highlight_relevant_cells заменена на новую
def apply_conditional_formatting(sheet):
    """
    Применяет условное форматирование к листу 'List'.
    - Окрашивает название модели в красный, если актуальность 'Нет'.
    """
    try:
        logging.info("Применение условного форматирования...")
        values = sheet.get_all_values()
        if not values:
            return

        header = values[0]
        try:
            model_col_idx = header.index('Модель')
            relevance_col_idx = header.index('Актуальность')
        except ValueError:
            logging.error("Не найдены необходимые колонки ('Модель', 'Актуальность') для форматирования.")
            return

        model_col_letter = chr(65 + model_col_idx)

        red_format = CellFormat(backgroundColor=Color(1, 0.8, 0.8))  # Светло-красный
        white_format = CellFormat(backgroundColor=Color(1, 1, 1))  # Белый (сброс)

        format_requests = []
        for i, row in enumerate(values[1:], start=2):  # Начинаем со второй строки
            # Проверяем, что в строке достаточно ячеек
            if len(row) > relevance_col_idx:
                relevance = row[relevance_col_idx].strip().lower()
                cell_range = f"{model_col_letter}{i}"
                if relevance == 'нет':
                    format_requests.append((cell_range, red_format))
                else:
                    format_requests.append((cell_range, white_format))

        if format_requests:
            # gspread_formatting работает эффективнее с пакетными запросами
            format_cell_ranges(sheet, format_requests)
            logging.info(f"Форматирование применено к {len(format_requests)} ячейкам.")

    except Exception as e:
        logging.error(f"[ERROR] Ошибка при условном форматировании: {e}")


### ИЗМЕНЕНИЕ ### Старая функция reset_relevance_column полностью заменена
def daily_price_and_relevance_update(force_check_date=None):
    """
    Выполняется ежедневно или по команде для обновления цен и статусов.
    Принимает force_check_date для симуляции проверки на определенную дату.
    """
    logging.info("[Scheduler] Запуск обновления цен и актуальности...")
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        credentials = Credentials.from_service_account_file('credentials.json', scopes=scope)
        gsheet_client = gspread.authorize(credentials)
        sheet = gsheet_client.open_by_key(GOOGLE_SHEET_ID).worksheet("List")

        values = sheet.get_all_values()
        if len(values) < 2:
            logging.info("[Scheduler] Лист 'List' пуст, обновление не требуется.")
            return

        original_header = values[0]
        df = pd.DataFrame(values[1:], columns=original_header)
        df.columns = [col.strip().lower() for col in df.columns]

        # ... (проверки и преобразования данных остаются теми же) ...
        df['минимальная цена'] = pd.to_numeric(df['минимальная цена'], errors='coerce')
        df['дата последней записи'] = pd.to_datetime(df['дата последней записи'], format='%d.%m.%Y', errors='coerce')

        status_col_normalized = PRICE_STATUS_COL.lower()
        if status_col_normalized not in df.columns:
            df[status_col_normalized] = STATUS_CURRENT
        df[status_col_normalized] = df[status_col_normalized].fillna(STATUS_CURRENT)

        moscow_tz = pytz.timezone("Europe/Moscow")

        # ### КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ###
        # Если нам передали дату для теста - используем ее.
        # Если нет (значит, это запуск по расписанию) - используем реальную текущую дату.
        if force_check_date:
            check_date = force_check_date
            logging.info(f"[Scheduler] ТЕСТОВЫЙ ЗАПУСК! Проверка относительно симулированной даты: {check_date}")
        else:
            check_date = datetime.now(moscow_tz).date()
            logging.info(f"[Scheduler] Плановый запуск. Проверка относительно реальной даты: {check_date}")

        mask_outdated = df['дата последней записи'].dt.date != check_date
        # ### КОНЕЦ ИЗМЕНЕНИЯ ###

        mask_needs_increase = (mask_outdated) & (df[status_col_normalized] == STATUS_CURRENT)

        logging.info(f"[Scheduler] Найдено устаревших позиций: {mask_outdated.sum()}")
        logging.info(f"[Scheduler] Найдено позиций для ПЕРВОГО повышения цены: {mask_needs_increase.sum()}")

        if mask_needs_increase.sum() > 0:
            df.loc[mask_needs_increase, 'минимальная цена'] += 5000
            df.loc[mask_needs_increase, status_col_normalized] = STATUS_INCREASED
            logging.info(f"[Scheduler] Цены для {mask_needs_increase.sum()} позиций были повышены.")

        if mask_outdated.sum() > 0:
            df.loc[mask_outdated, 'актуальность'] = 'Нет'

        df.loc[~mask_outdated, 'актуальность'] = 'Да'

        df.columns = original_header
        df['Дата последней записи'] = df['Дата последней записи'].dt.strftime('%d.%m.%Y')

        sheet.update([df.columns.values.tolist()] + df.fillna('').values.tolist(), value_input_option='USER_ENTERED')
        logging.info(f"[Scheduler] Обновление завершено. Проверено {len(df)} позиций.")

        apply_conditional_formatting(sheet)

    except Exception as e:
        logging.error(f"[ERROR] Критическая ошибка в обновлении: {e}", exc_info=True)


### ИЗМЕНЕНИЕ ### Обновляем планировщик
def scheduler_loop():
    """Цикл для выполнения задач по расписанию."""
    # Устанавливаем задачу на 13:00 по московскому времени
    moscow_tz = pytz.timezone("Europe/Moscow")
    schedule.every().day.at("13:00", moscow_tz).do(daily_price_and_relevance_update)
    logging.info(f"Планировщик настроен на ежедневный запуск в 13:00 (MSK).")

    while True:
        schedule.run_pending()
        time.sleep(60)


# Запускаем планировщик в отдельном потоке
threading.Thread(target=scheduler_loop, daemon=True).start()


def drop_table(update: Update, context: CallbackContext):
    try:
        raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("Raw")
        list_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List")

        raw_values = raw_sheet.get_all_values()
        if len(raw_values) > 1:
            raw_sheet.clear_basic_filter()
            raw_sheet.batch_clear([f"A2:Z{len(raw_values)}"])

        list_values = list_sheet.get_all_values()
        if len(list_values) > 1:
            list_sheet.clear_basic_filter()
            list_sheet.batch_clear([f"A2:Z{len(list_values)}"])

        update.message.reply_text("🗑 Таблицы *Raw* и *List* очищены (заголовки сохранены).",
                                  parse_mode='Markdown')
        logging.info("[Command] Таблицы очищены по команде /droptable")
    except Exception as e:
        logging.error(f"[ERROR] Ошибка при очистке таблиц: {e}")
        update.message.reply_text("⚠️ Не удалось очистить таблицы.")


def clear_messages(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    user_messages[uid] = []
    update.message.reply_text("🧹 Список сохранённых сообщений очищен.")


def start_form(update: Update, context: CallbackContext):
    update.message.reply_text("Введите имя поставщика:")
    return ASK_SUPPLIER


def run_update_command(update: Update, context: CallbackContext):
    """
    Принудительно запускает ежедневное обновление для тестирования,
    симулируя, что наступил СЛЕДУЮЩИЙ день.
    """
    update.message.reply_text("⏳ Запускаю тестовое обновление (симулирую следующий день)... Пожалуйста, подождите.")
    try:
        # ### КЛЮЧЕВОЕ ИЗМЕНЕНИЕ ###
        # Симулируем, что сегодня "завтрашний день"
        moscow_tz = pytz.timezone("Europe/Moscow")
        simulated_tomorrow = (datetime.now(moscow_tz) + timedelta(days=1)).date()

        # Вызываем основную функцию, передавая ей "поддельную" дату
        daily_price_and_relevance_update(force_check_date=simulated_tomorrow)
        # ### КОНЕЦ ИЗМЕНЕНИЯ ###

        update.message.reply_text("✅ Тестовое обновление завершено! Проверьте, пожалуйста, таблицу 'List'.")
    except Exception as e:
        logging.error(f"[TEST UPDATE] Ошибка при тестовом запуске: {e}", exc_info=True)
        update.message.reply_text(
            f"❌ Во время тестового обновления произошла ошибка. Подробности в логах.\n\nОшибка: {e}")


def main():
    updater = Updater(TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    # --- РЕГИСТРАЦИЯ КОМАНД ---
    # Основные команды, доступные всегда
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("droptable", drop_table))

    # Новая команда для тестирования ежедневного обновления
    dp.add_handler(CommandHandler("runupdate", run_update_command))

    # --- НАСТРОЙКА ДИАЛОГА (CONVERSATION) ---
    # Этот блок отвечает за пошаговый процесс "Сформировать -> Ввести имя поставщика"
    conv_handler = ConversationHandler(
        # Точки входа в диалог
        entry_points=[
            # Запускается по кнопке или текстовому сообщению "Сформировать" (без учета регистра)
            MessageHandler(Filters.regex(r'(?i)^Сформировать$'), start_form),
        ],
        # Состояния диалога
        states={
            # Состояние ASK_SUPPLIER: бот ожидает любое текстовое сообщение, которое не является командой
            ASK_SUPPLIER: [MessageHandler(Filters.text & ~Filters.command, receive_supplier)]
        },
        # Способы прервать диалог (в нашем случае их нет, но аргумент обязателен)
        fallbacks=[]
    )

    # Добавляем обработчик диалога в диспетчер
    dp.add_handler(conv_handler)

    # --- ОСТАЛЬНЫЕ ОБРАБОТЧИКИ ---
    # Эти обработчики работают, только если мы НЕ находимся внутри диалога
    # Обрабатывает нажатия кнопок "Таблица", "Очистить"
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_keyboard_input))
    # Обрабатывает сообщения с картинками и подписями (для сохранения в память)
    dp.add_handler(MessageHandler(Filters.photo & ~Filters.command, handle_message))

    logging.info("[Bot] Бот запущен и слушает сообщения...")
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
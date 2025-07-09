# telegram_supplier_bot.py
import re
import time
from rapidfuzz import fuzz, process
import logging
import json
import os
import openai
import pandas as pd
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler,
    Filters, CallbackContext, ConversationHandler
)
import gspread
from google.oauth2.service_account import Credentials

# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = '8138310577:AAGm6oKHlSD2KHCWfScMZE1lIUOunpBI-sg'
OPENAI_API_KEY = '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'
GOOGLE_SHEET_ID = '1rrjD_SpB79V0djuW-lDP_hIRptzdrITauRoybuJkoqA'

GOOGLE_SHEET_RAW = 'Raw'
GOOGLE_SHEET_CATALOG = 'Catalog'
CUSTOM_GROUPS_FILE = 'custom_groups.json'

openai.api_key = OPENAI_API_KEY

scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file('credentials.json', scopes=scope)
gsheet = gspread.authorize(credentials)

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

def normalize_name(name):
    name = name.lower()
    name = re.sub(r"\(.*?\)", "", name)  # убираем скобки (nano sim и т.п.)
    name = re.sub(r"\b(dual sim|nano sim|esim|iso|ansi)\b", "", name)
    name = re.sub(r"[^a-z0-9\s]", "", name)  # удаляем лишние символы
    name = re.sub(r"\s+", " ", name).strip()
    return name

def save_custom_groups(groups):
    with open(CUSTOM_GROUPS_FILE, "w", encoding="utf-8") as f:
        json.dump(groups, f, ensure_ascii=False, indent=2)

custom_groups = load_custom_groups()

def find_best_match(model, sku_df, threshold=93):
    matches = process.extractOne(model, sku_df['model'], scorer=fuzz.token_sort_ratio)
    if matches and matches[1] >= threshold:
        matched_model = matches[0]
        sku_row = sku_df[sku_df['model'] == matched_model]
        return sku_row['market_sku'].values[0]
    return None

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

def normalize_with_ai(messages, supplier):
    text = "".join(messages)
    prompt = f"""
    Ты получаешь список товаров от поставщика. Верни JSON-массив объектов с полями:
    - model
    - memory
    - color
    - price
    - quantity
    - supplier (всегда "{supplier}")
    - datetime (в формате YYYY-MM-DD HH:MM, всегда 0000-00-00 00:00)

    По информации о стране или обозначениях в названии модели определи **тип SIM-карт** и добавь его в **название модели** в скобках. Примеры:

    — Если США / LL/A / American version — это **(dual esim)**  
    — Если Россия / RU/A — это **(nano sim + esim)**  
    — Если Гонконг / ZP/A — это **(2 nano sim)**  
    — Если Китай / CH/A — это **(2 nano sim)**  
    — Если Европа / F/A, D/A, B/A и т.п. — это **(nano sim + esim)**  
    — Если Тайвань / TA/A — это **(nano sim + esim)**  
    — Если информации нет тут — ищи её сам.
    — Если это MacBook, то попробуй понять какая клавиатура (ANSI или ISO) - она зависит от страны. например США и Индия имеют клавиатуру ANSI. Поищи информацию и пойми что за клавиатура. Вместо флага или сим добавь клавиатуру (ANSI) или (ISO) в конце модели. Если информации нет, то скорее всего США - т.е. ANSI.

    Примеры:

    "iPhone 15 Pro Max 256GB ZP/A" → model: "iPhone 15 Pro Max (2 nano sim)"
    "iPhone 13 LL/A" → model: "iPhone 13 (dual esim)"
    "iPhone 14 Pro Max" → model: "iPhone 14 Pro Max (nano sim + esim)"

    Нормализуй цену в рублях. Если пишут 71.2, значит 71200. Если указана цена с пробелами, точками или запятыми — корректно распознай. Не делай ошибок в JSON.
    Если это MacBook, то в начале модели должно быть написано "MacBook"
    Если это Iphone, то в начале модели должно быть написано "IPhone"
    
    Если возле товара крестик или сообщение перечёркнуто - не добавляй. В остальных случаях добавляй.
    Это может быть любой товар, не только от Apple или любых других компаний. Любой.
    Некоторые поставщики дают цену ОПТОМ, например от 10 штук одна цена, от 20 другая. Отсеивай предложения оптом, и считывай только цену поштучно.
    Некоторые поставщики не пишут на каждый товар фирму производителя (например Dyson). Дописывай фирму в начале перед названием модели.
    Если это не телефон, то не нужно добавлять информацию о симкарте.
    
    Дополнительные инструкции:
- Если цена указана со знаком минус (например, -49999), это НЕ означает, что товара нет. Просто убери знак минус и считай, что цена = 49999.
- Если в строке не указано явно название бренда (например Dyson), но модель содержит "Absolute", "V11", "SV", "Detect" и т.п. — добавь "Dyson" в начало модели.
- Если есть флаг, например 🇬🇧, используй его для определения страны (например 🇬🇧 — это Великобритания, nano sim + esim или клавиатура ISO).
- Не отбрасывай товары, даже если они записаны одной строкой без уточнений — старайся распознать модель, цену и как минимум supplier.
- Если нет количества — считай, что quantity = 1.

    Вот товары:
    {text}
    """

    try:
        print("[AI] Отправляем в ChatGPT...")
        print(f"[AI] PROMPT:{prompt}")
        response = openai.ChatCompletion.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Ты помощник по нормализации товаров поставщиков."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )
        print("[AI] Ответ получен")
        content = response['choices'][0]['message']['content'].strip()
        print(f"[AI] CONTENT RAW:{content}")
        if not content or not (content.startswith("[") or content.startswith("{")):
            raise ValueError("AI вернул пустой или невалидный ответ")
        return json.loads(content)
    except Exception as e:
        logging.error(f"OpenAI Error: {e}")
        return []

import hashlib
def normalize_for_match(text):
    text = text.lower()
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\d{2,4}\s?(gb|tb)", "", text)
    text = re.sub(r"\b(black|blue|yellow|white|red|green|purple|gold|silver|gray|pink|ultramarine)\b", "", text)
    text = re.sub(r"\b(nano sim|dual sim|esim|iso|ansi)\b", "", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()
def generate_model_id(model: str, memory: str, color: str, country: str) -> str:
    base_string = f"{model}|{memory}|{color}|{country}".lower().strip()
    return hashlib.sha1(base_string.encode('utf-8')).hexdigest()[:10]


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
def parse_summary_model(summary_model):
    """
    Делит строку типа "IPhone 14 Plus (dual esim) 128 Yellow" на части
    """
    s = summary_model
    s = s.replace("IPhone", "iPhone").replace("Iphone", "iPhone")  # нормализуем регистр
    model = None
    memory = None
    color = None
    country = None

    # Убрать скобки
    s = re.sub(r"\(.*?\)", "", s).strip()
    # Найти память (GB, TB)
    mem = re.findall(r"(\d+\s?(gb|tb))", s.lower())
    if mem:
        memory = mem[0][0].replace(" ", "").upper()  # типа '128GB'
        s = s.replace(mem[0][0], "")
    # Найти цвет (последнее слово)
    words = s.split()
    if len(words) > 1:
        color = words[-1].capitalize()
        s = " ".join(words[:-1])
    model = s.strip().replace("  ", " ")
    return {
        "model": model,
        "memory": memory,
        "color": color,
        "country": country
    }

def parse_sim_type(model):
    import re
    if not isinstance(model, str):
        return ''
    match = re.search(r'\((.*?)\)', model)
    if match:
        sim = match.group(1)
        for pattern in ['nano sim + esim', 'dual esim', '2 nano sim']:
            if pattern in sim.lower():
                return pattern
    return ''

def normalize_str(s):
    # Очищает строку, приводит к нижнему регистру, удаляет лишние символы, убирает все пробелы подряд
    return re.sub(r'\s+', ' ', str(s).lower().strip().replace("-", " ")).strip()


def find_sku(summary_row, sku_df):
    # summary_row — строка из summary DataFrame
    # sku_df — DataFrame из art.csv (SKU база)
    model, memory, color, _ = split_summary_model(summary_row['model'])
    n_model = normalize_str(model)
    n_memory = normalize_str(memory)
    n_color = normalize_str(color)
    n_sim = normalize_str(parse_sim_type(summary_row['model']))

    # Строгое совпадение: модель, память, цвет, симка
    match = sku_df[
        (sku_df['n_model'] == n_model) &
        (sku_df['n_memory'] == n_memory) &
        (sku_df['n_color'] == n_color) &
        (sku_df['n_sim'] == n_sim)
    ]
    if not match.empty:
        return str(match.iloc[0]['market_sku'])

    # Без цвета: модель, память, симка
    match = sku_df[
        (sku_df['n_model'] == n_model) &
        (sku_df['n_memory'] == n_memory) &
        (sku_df['n_sim'] == n_sim)
    ]
    if not match.empty:
        return str(match.iloc[0]['market_sku'])

    return "—"
def send_table_link(update: Update, context: CallbackContext):
    url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}"
    update.message.reply_text(f"📊 Вот ссылка на таблицу:\n{url}")

def split_summary_model(model_str):
    import re
    if not isinstance(model_str, str):
        return '', '', '', ''
    main = re.sub(r'\(.*?\)', '', model_str).strip()
    parts = main.split()
    model = []
    memory = ''
    color = ''
    for part in parts:
        if re.fullmatch(r'(\d+|1tb|2tb|512gb|256gb|128gb)', part.lower()):
            memory = part
        elif part.lower() in [
            "black", "white", "blue", "yellow", "gold", "green", "purple", "red", "gray", "pink", "ultramarine", "natural", "teal"
        ]:
            color = part
        else:
            model.append(part)
    return ' '.join(model).strip(), memory, color, ''

def update_google_sheets(products):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_RAW)

    rows = []
    for p in products:
        model = p.get('model', '').strip()
        country = 'USA' if '🇺🇸' in model or 'LL/A' in model else 'N/A'

        print(f"[DEBUG] Обрабатывается товар: {p}")
        try:
            price = float(p.get('price') or 0)
            quantity = int(p.get('quantity') or 1)

            if price <= 0 or quantity <= 0:
                print(f"[SKIP] Пропущено: цена={price}, кол-во={quantity}, модель={model}")
                continue

            rows.append([
                model, p.get('memory'), p.get('color'),
                price, quantity,
                p.get('supplier'),
                datetime.now().strftime("%Y-%m-%d %H:%M"), country
            ])
        except Exception as e:
            print(f"[ERROR] Ошибка при обработке товара: {p} — {e}")

    if not raw_sheet.get_all_values():
        header = ['model', 'memory', 'color', 'price', 'quantity', 'supplier', 'datetime', 'country']
        print(f"[DEBUG] Первый товар: {products[0] if products else 'Пусто'}")
        raw_sheet.append_row(header)
        print("[Google Sheets] Заголовки добавлены в Raw")
    print(f"[DEBUG] Первый товар: {products[0] if products else 'Пусто'}")
    print(f"[Google Sheets] Добавляем {len(rows)} строк в Raw...")
    raw_sheet.append_rows(rows, value_input_option='USER_ENTERED')



    # === Обновляем лист List ===
    data = raw_sheet.get_all_values()
    if not data or len(data) < 2:
        print("[WARNING] Лист Raw пуст или недостаточно строк.")
        return

    headers = data[0]
    headers = [h.strip() for h in headers]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)
    print(f"[DEBUG] df rows count: {len(df)}")
    df.columns = df.columns.str.strip()
    if 'country' not in df.columns:
        df['country'] = 'N/A'
    df['model'] = df['model'].astype(str).str.strip()
    df = df[df['model'] != '']

    for col in ['price']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    print(f"[DEBUG] Строк с ценой: {len(df[df['price'].notnull()])}")
    print("[DEBUG] Столбцы в DataFrame:", df.columns.tolist())
    print("[DEBUG] Примеры данных:")
    print(df[['model', 'price', 'supplier']].head(10).to_string(index=False))

    summary_rows = []

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    if 'ID' not in df.columns:
        df['ID'] = df.apply(lambda row: generate_model_id(
            row.get('model', ''),
            row.get('memory', ''),
            row.get('color', ''),
            row.get('country', '')
        ), axis=1)
    else:
        df['ID'] = df['ID'].astype(str).fillna('').replace('', pd.NA)

        mask_missing = df['ID'].isna()
        df.loc[mask_missing, 'ID'] = df[mask_missing].apply(lambda row: generate_model_id(
            row.get('model', ''),
            row.get('memory', ''),
            row.get('color', ''),
            row.get('country', '')
        ), axis=1)

    for id_value, group in df[df['price'].notnull()].groupby('ID'):
        group = group.sort_values(by='datetime', ascending=False)
        latest_date = group.iloc[0]['datetime']
        today_group = group[group['datetime'].dt.date == latest_date.date()]

        if not today_group.empty:
            cheapest = today_group.loc[today_group['price'].idxmin()]

            base = cheapest['model'].strip()
            if cheapest['memory'] not in base:
                base += f" {cheapest['memory']}"
            if cheapest['color'] not in base:
                base += f" {cheapest['color']}"
            if cheapest['country'] != 'N/A':
                base += " 🇺🇸"

            display_name = base.strip()
            suppliers = today_group[
                (today_group['price'] == cheapest['price']) &
                (today_group['datetime'] == cheapest['datetime'])
                ]['supplier'].dropna().unique()

            summary_rows.append([
                display_name,
                cheapest['price'],
                ', '.join(suppliers),
                cheapest['model'],
                cheapest['memory'],
                cheapest['color'],
                cheapest['country'],
                id_value  # добавляем ID как столбец
            ])

    summary = pd.DataFrame(summary_rows, columns=[
        'model', 'price', 'supplier', 'raw_model', 'raw_memory', 'raw_color', 'raw_country', 'ID'
    ])
    sku_df = pd.read_csv("art.csv")

    # Нормализуем строки для поиска
    sku_df['n_model'] = sku_df['model'].map(normalize_str)
    sku_df['n_memory'] = sku_df['memory'].map(lambda x: normalize_str(str(x)))
    sku_df['n_color'] = sku_df['color'].map(normalize_str)
    if 'sim_type' in sku_df.columns:
        sku_df['n_sim'] = sku_df['sim_type'].map(normalize_str)
    else:
        sku_df['n_sim'] = ''

    if 'SKU' not in summary.columns:
        summary['SKU'] = ""

    for idx, row in summary.iterrows():
        model, memory, color, _ = split_summary_model(row['model'])
        n_model = normalize_str(model)
        n_memory = normalize_str(memory)
        n_color = normalize_str(color)
        n_sim = normalize_str(parse_sim_type(row['model']))

        # Ищем строго по всем 4м: модель, память, цвет, симка
        match = sku_df[
            (sku_df['n_model'] == n_model) &
            (sku_df['n_memory'] == n_memory) &
            (sku_df['n_color'] == n_color) &
            (sku_df['n_sim'] == n_sim)
            ]
        if not match.empty:
            summary.at[idx, 'SKU'] = str(match.iloc[0]['market_sku'])
            print(
                f"[MATCH] {row['model']} → {n_model}|{n_memory}|{n_color}|{n_sim} → {match.iloc[0]['model']}|{match.iloc[0]['memory']}|{match.iloc[0]['color']}|{match.iloc[0]['sim_type']} → SKU={match.iloc[0]['market_sku']}")
            continue

        # Ищем без цвета (чуть менее строгий вариант)
        match = sku_df[
            (sku_df['n_model'] == n_model) &
            (sku_df['n_memory'] == n_memory) &
            (sku_df['n_sim'] == n_sim)
            ]
        if not match.empty:
            summary.at[idx, 'SKU'] = str(match.iloc[0]['market_sku'])
            print(
                f"[MATCH*] {row['model']} → {n_model}|{n_memory}|*|{n_sim} → {match.iloc[0]['model']}|{match.iloc[0]['memory']}|*|{match.iloc[0]['sim_type']} → SKU={match.iloc[0]['market_sku']}")
            continue

        print(f"[NO MATCH] {row['model']} → {n_model}|{n_memory}|{n_color}|{n_sim}")
        summary.at[idx, 'SKU'] = '—'

    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    last_dates = df.groupby(['model', 'memory', 'color', 'country'])['datetime'].max().reset_index()
    last_dates['date_str'] = last_dates['datetime'].dt.strftime('%d.%m')

    summary['Дата последней записи'] = summary.apply(
        lambda row: next(
            (d['date_str'] for _, d in last_dates.iterrows()
             if d['model'] == row['raw_model']
             and d['memory'] == row['raw_memory']
             and d['color'] == row['raw_color']
             and d['country'] == row['raw_country']),
            '—'
        ),
        axis=1
    )
    summary.sort_values(by='model', key=lambda col: col.str.lower(), inplace=True)
    output = [['Модель', 'Минимальная цена', 'Поставщик', 'Дата последней записи', 'Актуальность']]
    today = datetime.now().strftime('%d.%m')
    for _, row in summary.iterrows():
        актуальность = 'Да' if row['Дата последней записи'] == today else 'Нет'
        output.append([row['model'], row['price'], row['supplier'], row['Дата последней записи'], актуальность])

    try:
        gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List").clear()
    except:
        gsheet.open_by_key(GOOGLE_SHEET_ID).add_worksheet(title="List", rows="1000", cols="20")

    list_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List")
    existing = list_sheet.get_all_values()
    if not existing:
        list_sheet.update([output[0]])  # только заголовки
        existing_rows = {}
    else:
        header = existing[0]
        id_index = header.index("ID") if "ID" in header else -1
        existing_rows = {
            row[id_index]: row for row in existing[1:] if len(row) > id_index
        }

    # Сливаем обновлённые строки с сохранением ID
    new_output = [output[0] + ["ID", "SKU"]]
    for _, row in summary.iterrows():
        model_id = generate_model_id(
            row['raw_model'],
            row['raw_memory'],
            row['raw_color'],
            row['raw_country']
        )
        актуальность = 'Да' if row['Дата последней записи'] == today else 'Нет'
        new_output.append([
            row['model'],
            row['price'],
            row['supplier'],
            row['Дата последней записи'],
            актуальность,
            model_id,
            row.get('SKU', '')
        ])

    list_sheet.clear()
    list_sheet.update(new_output)
    print("[Google Sheets] Лист List обновлён с минимальными ценами.")
    highlight_relevant_cells(list_sheet)

def handle_message(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    # Добавим поддержку текста из caption
    text = update.message.text or update.message.caption

    print(f"[input] Сообщение от пользователя {uid}: {text}")

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
    messages = [m.strip() for m in messages if m.strip()]  # убираем пустые
    messages = list(set(messages))  # убираем дубликаты

    print(f"[DEBUG] Очишенные сообщения: {messages}")


    if not messages:
        update.message.reply_text("Нет сообщений.")
        return ConversationHandler.END

    print(f"[AI] Обработка сообщений от поставщика '{supplier}'...")
    update.message.reply_text(f"Обрабатываю {len(messages)} сообщений...")
    products = normalize_with_ai(messages, supplier)
    print(f"[DEBUG] Получено товаров от AI: {len(products)}")
    print(f"[DEBUG] Продукты: {products}")

    unknown = [p['model'] for p in products if not get_device_group(p['model'])]

    for model in set(unknown):
        custom_groups[model] = model
        update.message.reply_text(f"✅ Группа *{model}* создана.", parse_mode='Markdown')

    if unknown:
        save_custom_groups(custom_groups)

    update_google_sheets(products)
    update.message.reply_text(f"✅ Обработано: {len(products)} товаров")
    user_messages[uid] = []
    return ConversationHandler.END

def ask_next_group(update: Update, context: CallbackContext, uid=None):
    if uid is None:
        uid = update.effective_user.id

    pending = pending_products.get(uid)
    if not pending or not pending['pending_models']:
        context.bot.send_message(chat_id=uid, text="✅ Все новые группы обработаны.")
        update_google_sheets(pending['products'])
        user_messages[uid] = []
        pending_products.pop(uid, None)
        return

    model = pending['pending_models'].pop(0)
    print(f"[Группы] Неизвестная модель: {model} — спрашиваем пользователя.")
    keyboard = [
        [InlineKeyboardButton("Создать группу", callback_data=f"create_group:{hash(model)}")],
        [InlineKeyboardButton("Пропустить", callback_data=f"skip_group:{hash(model)}")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    context.bot.send_message(chat_id=uid, text=f"Модель *{model}* не распознана. Создать новую группу?",
                             parse_mode='Markdown', reply_markup=markup)

def handle_group_decision(update: Update, context: CallbackContext):
    query = update.callback_query
    uid = query.from_user.id
    data = query.data
    query.answer()

    if data.startswith("create_group:"):
        model = data.split(":", 1)[1]
        for m in pending_products[uid]['pending_models']:
            if hash(m) == int(model):
                custom_groups[m] = m
        save_custom_groups(custom_groups)
        print(f"[Группы] Группа создана: {model}")
        query.edit_message_text(f"✅ Группа *{model}* создана.", parse_mode='Markdown')

    elif data.startswith("skip_group:"):
        model = data.split(":", 1)[1]
        print(f"[Группы] Модель пропущена: {model}")
        query.edit_message_text(f"⏭ Модель *{model}* пропущена.", parse_mode='Markdown')
        pending_products[uid]['products'] = [p for p in pending_products[uid]['products'] if hash(p['model']) != int(model)]

    ask_next_group(update, context, uid)

def highlight_relevant_cells(sheet):
    try:
        from gspread_formatting import CellFormat, Color, format_cell_range
        from itertools import groupby

        values = sheet.get_all_values()
        if not values:
            return

        header = values[0]
        data = values[1:]
        if 'Актуальность' not in header:
            return

        relevance_index = header.index('Актуальность')
        col_letter = chr(65 + relevance_index)

        green = CellFormat(backgroundColor=Color(0.8, 1, 0.8))
        green_cells = [f"{col_letter}{idx}" for idx, row in enumerate(data, start=2)
                       if len(row) > relevance_index and row[relevance_index].strip().lower() == 'да']

        def compress_ranges(cells):
            ranges = []
            for k, g in groupby(enumerate(cells), lambda ix: ix[0] - int(ix[1][len(col_letter):])):
                group = list(g)
                if len(group) == 1:
                    ranges.append(group[0][1])
                else:
                    start = group[0][1]
                    end = group[-1][1]
                    ranges.append(f"{start}:{end}")
            return ranges

        for rng in compress_ranges(green_cells):
            format_cell_range(sheet, rng, green)

    except Exception as e:
        print(f"[ERROR] Ошибка при закраске актуальных ячеек: {e}")

def reset_relevance_column():
    try:
        sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List")
        values = sheet.get_all_values()
        if not values:
            return

        header = values[0]
        data = values[1:]

        if 'Актуальность' not in header:
            header.append('Актуальность')
            for row in data:
                row.append('Нет')
        else:
            relevance_index = header.index('Актуальность')
            for row in data:
                if len(row) <= relevance_index:
                    row += ['Нет']
                else:
                    row[relevance_index] = 'Нет'

        sheet.update([header] + data)

        # Форматирование цветов
        from gspread_formatting import CellFormat, Color, format_cell_range
        from gspread_formatting import set_frozen

        green = CellFormat(backgroundColor=Color(0.8, 1, 0.8))
        red = CellFormat(backgroundColor=Color(1, 0.8, 0.8))

        relevance_index = header.index('Актуальность')
        green = CellFormat(backgroundColor=Color(0.8, 1, 0.8))
        red = CellFormat(backgroundColor=Color(1, 0.8, 0.8))

        relevance_index = header.index('Актуальность')
        cell_range = f"{chr(65 + relevance_index)}2:{chr(65 + relevance_index)}{len(data)+1}"

        color_values = []
        for row in data:
            if len(row) > relevance_index and row[relevance_index].strip().lower() == 'да':
                color_values.append(green)
            else:
                color_values.append(red)

        from gspread_formatting import format_cell_range

        green = CellFormat(backgroundColor=Color(0.8, 1, 0.8))
        red = CellFormat(backgroundColor=Color(1, 0.8, 0.8))

        col_letter = chr(65 + relevance_index)
        total_rows = len(data)

        green_range = []
        red_range = []

        for idx, row in enumerate(data, start=2):
            val = row[relevance_index].strip().lower() if len(row) > relevance_index else 'нет'
            if val == 'да':
                green_range.append(f"{col_letter}{idx}")
            else:
                red_range.append(f"{col_letter}{idx}")

        from itertools import groupby

        def compress_ranges(cells):
            ranges = []
            for k, g in groupby(enumerate(cells), lambda ix: ix[0] - int(ix[1][len(col_letter):])):
                group = list(g)
                if len(group) == 1:
                    ranges.append(group[0][1])
                else:
                    start = group[0][1]
                    end = group[-1][1]
                    ranges.append(f"{start}:{end}")
            return ranges

        for rng in compress_ranges(green_range):
            format_cell_range(sheet, rng, green)
        for rng in compress_ranges(red_range):
            format_cell_range(sheet, rng, red)

        print("[List] Актуальность сброшена и переформатирована.")
    except Exception as e:
        print(f"[ERROR] Ошибка при сбросе актуальности: {e}")

import threading
import schedule

def zero_command(update: Update, context: CallbackContext):
    reset_relevance_column()
    update.message.reply_text("✅ Актуальность всех моделей сброшена.")

def scheduler_loop():
    schedule.every().day.at("00:00").do(reset_relevance_column)
    while True:
        schedule.run_pending()
        time.sleep(60)

threading.Thread(target=scheduler_loop, daemon=True).start()

def drop_table(update: Update, context: CallbackContext):
    try:
        raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("Raw")
        list_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet("List")

        # Очистка данных, но оставляем заголовки
        raw_values = raw_sheet.get_all_values()
        if raw_values:
            raw_sheet.batch_clear([f"A2:Z{len(raw_values)}"])
        list_values = list_sheet.get_all_values()
        if list_values:
            list_sheet.batch_clear([f"A2:Z{len(list_values)}"])

        update.message.reply_text("🗑 Таблицы *Raw* и *List* очищены (заголовки и форматирование сохранены).", parse_mode='Markdown')
        print("[Command] Таблицы очищены по команде /droptable")
    except Exception as e:
        print(f"[ERROR] Ошибка при очистке таблиц: {e}")
        update.message.reply_text("⚠️ Не удалось очистить таблицы.")

def clear_messages(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    user_messages[uid] = []
    update.message.reply_text("🧹 Список сохранённых сообщений очищен.")

def start_form(update: Update, context: CallbackContext):
    update.message.reply_text("Введи имя поставщика:")
    return ASK_SUPPLIER

def main():
    updater = Updater(TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("form", start_form))
    dp.add_handler(CommandHandler("zero", zero_command))
    dp.add_handler(CommandHandler("droptable", drop_table))

    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_callback, pattern="start_form"),
            MessageHandler(Filters.regex("^(Сформировать)$"), start_form),  # <== ЭТО ДОБАВЬ
        ],
        states={
            ASK_SUPPLIER: [MessageHandler(Filters.all & ~Filters.command, receive_supplier)]
        },
        fallbacks=[]
    )

    dp.add_handler(conv_handler)
    dp.add_handler(MessageHandler(Filters.caption & ~Filters.command, handle_keyboard_input))
    dp.add_handler(CallbackQueryHandler(handle_group_decision, pattern="^(create_group|skip_group):"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_keyboard_input))

    print("[Bot] Бот запущен и слушает сообщения...")
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()

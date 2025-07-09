# telegram_supplier_bot.py

import logging
import json
import os
import pandas as pd
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Updater, CommandHandler, MessageHandler, CallbackQueryHandler,
    Filters, CallbackContext, ConversationHandler
)
import gspread
from google.oauth2.service_account import Credentials

# === НАСТРОЙКИ ===
TELEGRAM_TOKEN = '8138310577:AAGm6oKHlSD2KHCWfScMZE1lIUOunpBI-sg'
GOOGLE_SHEET_ID = '1rrjD_SpB79V0djuW-lDP_hIRptzdrITauRoybuJkoqA'

GOOGLE_SHEET_RAW = 'Raw'
GOOGLE_SHEET_CATALOG = 'Catalog'
CUSTOM_GROUPS_FILE = 'custom_groups.json'

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

# === ЗАГЛУШКА ДЛЯ НОРМАЛИЗАЦИИ ===
def normalize_with_ai(messages, supplier):
    print("[MOCK AI] Возвращаю заглушку вместо запроса в OpenAI API")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    return [
        {
            "model": "MacBook Air 13 M4",
            "memory": "24/512",
            "color": "Starlight",
            "price": 117500,
            "quantity": 2,
            "currency": "RUB",
            "supplier": supplier,
            "datetime": now
        },
        {
            "model": "Sony PlayStation 5 Digital",
            "memory": None,
            "color": None,
            "price": 56000,
            "quantity": 1,
            "currency": "RUB",
            "supplier": supplier,
            "datetime": now
        }
    ]

def update_google_sheets(products):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    raw_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_RAW)

    rows = []
    for p in products:
        rows.append([
            p.get('model'), p.get('memory'), p.get('color'),
            float(p.get('price', 0)), int(p.get('quantity', 1)),
            p.get('currency', 'RUB'), p.get('supplier'), p.get('datetime', now)
        ])

    print(f"[Google Sheets] Добавляем {len(rows)} строк в Raw...")
    raw_sheet.append_rows(rows, value_input_option='USER_ENTERED')

    df = pd.DataFrame(raw_sheet.get_all_records())
    df['group'] = df['model'].apply(get_device_group)

    catalog = df.groupby(['model', 'memory', 'color']).agg({
        'price': ['min', 'max'],
        'supplier': lambda x: ', '.join(set(x)),
        'group': 'first'
    }).reset_index()

    catalog.columns = ['model', 'memory', 'color', 'min_price', 'max_price', 'suppliers', 'group']
    catalog = catalog.sort_values(by='group')

    try:
        gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_CATALOG).clear()
    except:
        gsheet.open_by_key(GOOGLE_SHEET_ID).add_worksheet(title=GOOGLE_SHEET_CATALOG, rows="1000", cols="20")

    print("[Google Sheets] Обновляем Catalog...")
    catalog_sheet = gsheet.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_CATALOG)
    catalog_sheet.update([catalog.columns.values.tolist()] + catalog.values.tolist())

def handle_message(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    text = update.message.text
    print(f"[input] Сообщение от пользователя {uid}: {text}")
    user_messages.setdefault(uid, []).append(text)
    update.message.reply_text("✅ Сообщение сохранено.")

def start_form(update: Update, context: CallbackContext):
    keyboard = [[InlineKeyboardButton("Сформировать", callback_data="start_form")]]
    markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Нажми кнопку, когда готов:", reply_markup=markup)

def button_callback(update: Update, context: CallbackContext):
    update.callback_query.answer()
    update.callback_query.message.reply_text("Введи имя поставщика:")
    return ASK_SUPPLIER

def receive_supplier(update: Update, context: CallbackContext):
    supplier = update.message.text
    uid = update.effective_user.id
    messages = user_messages.get(uid, [])

    if not messages:
        update.message.reply_text("Нет сообщений.")
        return ConversationHandler.END

    print(f"[AI] Обработка сообщений от поставщика '{supplier}'...")
    update.message.reply_text(f"Обрабатываю {len(messages)} сообщений...")
    products = normalize_with_ai(messages, supplier)

    unknown = [p['model'] for p in products if not get_device_group(p['model'])]

    if unknown:
        pending_products[uid] = {'products': products, 'pending_models': list(set(unknown))}
        ask_next_group(update, context, uid)
        return ConversationHandler.END

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
        [InlineKeyboardButton("Создать группу", callback_data=f"create_group:{model}")],
        [InlineKeyboardButton("Пропустить", callback_data=f"skip_group:{model}")]
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
        custom_groups[model] = model
        save_custom_groups(custom_groups)
        print(f"[Группы] Группа создана: {model}")
        query.edit_message_text(f"✅ Группа *{model}* создана.", parse_mode='Markdown')

    elif data.startswith("skip_group:"):
        model = data.split(":", 1)[1]
        print(f"[Группы] Модель пропущена: {model}")
        query.edit_message_text(f"⏭ Модель *{model}* пропущена.", parse_mode='Markdown')
        pending_products[uid]['products'] = [p for p in pending_products[uid]['products'] if p['model'] != model]

    ask_next_group(update, context, uid)

def main():
    updater = Updater(TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start_form))
    dp.add_handler(CommandHandler("form", start_form))

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_callback, pattern="start_form")],
        states={ASK_SUPPLIER: [MessageHandler(Filters.text & ~Filters.command, receive_supplier)]},
        fallbacks=[]
    )
    dp.add_handler(conv_handler)
    dp.add_handler(CallbackQueryHandler(handle_group_decision, pattern="^(create_group|skip_group):"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))

    print("[Bot] Бот запущен и слушает сообщения...")
    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()

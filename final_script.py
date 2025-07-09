# ——— НАЧАЛО ФАЙЛА script.py ———
import logging
import json
import re
import pandas as pd

def normalize_str(s):
    return str(s).strip().lower().replace("gb", "").replace(" ", "")

def parse_sim_type(model):
    sim_types = ["dual esim", "nano sim + esim", "2 nano sim"]
    for sim in sim_types:
        if sim in model.lower():
            return sim
    return ""

def split_summary_model(model):
    parts = model.lower().split()
    memory = next((p for p in parts if 'gb' in p or '/' in p), '')
    color = next((p for p in parts if p not in ['iphone', 'pro', 'max', 'plus', 'mini'] and not memory in p), '')
    base_model = model.lower().replace(memory, '').replace(color, '').strip()
    return base_model, memory, color, ''

def parse_response(response_text):
    try:
        print("[DEBUG] AI raw response:", response_text)
        json_match = re.search(r"```(?:json)?\s*(\[\s*{.*?}\s*])\s*```", response_text, re.DOTALL)
        if not json_match:
            json_match = re.search(r"(\[\s*{.*?}\s*])", response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
            return json.loads(json_str)
        else:
            print("[ERROR] Не удалось найти JSON в ответе")
            return []
    except Exception as e:
        print("[ERROR] parse_response():", e)
        return []

def find_sku(summary_rows, sku_path="art.csv"):
    summary = pd.DataFrame(summary_rows, columns=[
        'model', 'price', 'supplier', 'raw_model', 'raw_memory', 'raw_color', 'raw_country', 'ID'
    ])
    sku_df = pd.read_csv(sku_path)

    sku_df['n_model'] = sku_df['model'].map(normalize_str)
    sku_df['n_memory'] = sku_df['memory'].map(lambda x: normalize_str(str(x)))
    sku_df['n_color'] = sku_df['color'].map(normalize_str)
    sku_df['n_sim'] = sku_df['sim_type'].map(normalize_str) if 'sim_type' in sku_df.columns else ''

    if 'SKU' not in summary.columns:
        summary['SKU'] = ""

    for idx, row in summary.iterrows():
        model, memory, color, _ = split_summary_model(row['model'])
        n_model = normalize_str(model)
        n_memory = normalize_str(memory)
        n_color = normalize_str(color)
        n_sim = normalize_str(parse_sim_type(row['model']))

        match = sku_df[
            (sku_df['n_model'] == n_model) &
            (sku_df['n_memory'] == n_memory) &
            (sku_df['n_color'] == n_color) &
            (sku_df['n_sim'] == n_sim)
        ]
        if not match.empty:
            summary.at[idx, 'SKU'] = str(match.iloc[0]['market_sku'])
            print(f"[MATCH] {row['model']} → {n_model}|{n_memory}|{n_color}|{n_sim} → {match.iloc[0]['market_sku']}")
            continue

        match = sku_df[
            (sku_df['n_model'] == n_model) &
            (sku_df['n_memory'] == n_memory) &
            (sku_df['n_sim'] == n_sim)
        ]
        if not match.empty:
            summary.at[idx, 'SKU'] = str(match.iloc[0]['market_sku'])
            print(f"[MATCH*] {row['model']} → {n_model}|{n_memory}|*|{n_sim} → {match.iloc[0]['market_sku']}")
            continue

        print(f"[NO MATCH] {row['model']} → {n_model}|{n_memory}|{n_color}|{n_sim}")
        summary.at[idx, 'SKU'] = '—'

    return summary

# ——— КОНЕЦ ФАЙЛА ———
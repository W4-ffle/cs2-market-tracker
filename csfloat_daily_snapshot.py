# csfloat_daily_snapshot.py
# pip install "pymongo[srv]" requests python-dotenv

import os, re
from datetime import datetime
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
CSFLOAT_URL = os.getenv("CSFLOAT_URL", "https://csfloat.com/api/v1/listings/price-list")
CSFLOAT_TIMEOUT = 60

if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
csfloat_daily_col = db["csfloat_daily_prices"]
all_item_names_col = db["all_item_names"]

def parse_price_to_float(price_val):
    if price_val is None:
        return None
    try:
        return float(price_val)
    except:
        return None

def extract_condition(item_name):
    condition = None
    if "(" in item_name and ")" in item_name:
        condition = item_name.split("(")[-1].split(")")[0].strip()
    return condition

def csfloat_fetch_price_list():
    resp = requests.get(CSFLOAT_URL, timeout=CSFLOAT_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def run_daily_csfloat_with_id():
    print("Starting CSFloat daily snapshot with item_id:", datetime.utcnow().isoformat())
    try:
        items = csfloat_fetch_price_list()
    except Exception as e:
        print("Failed to fetch CSFloat price list:", e)
        return

    now = datetime.utcnow()
    date_for_daily = datetime(now.year, now.month, now.day)
    total_saved = 0

    for item in items:
        market_name = item.get("market_hash_name")
        if not market_name:
            continue

        condition = extract_condition(market_name)
        price_val = parse_price_to_float(item.get("min_price"))

        # get the all_item_names _id
        all_item_doc = all_item_names_col.find_one({"raw_name": market_name})
        if not all_item_doc:
            continue  # skip if no mapping
        item_id = all_item_doc["_id"]

        csfloat_daily_col.update_one(
            {"item_id": item_id, "condition": condition, "date": date_for_daily},
            {"$set": {
                "item_id": item_id,
                "item_name": market_name,
                "condition": condition,
                "price": price_val,
                "quantity": item.get("qty"),
                "raw_cs": item,
                "date": date_for_daily,
                "fetched_at": now
            }},
            upsert=True
        )
        total_saved += 1

    print(f"CSFloat daily snapshot completed. Total items saved: {total_saved}")

if __name__ == "__main__":
    run_daily_csfloat_with_id()

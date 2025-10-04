# fetch_prices_all_conditions.py
# pip install "pymongo[srv]" requests python-dotenv

import os
import time
from urllib.parse import quote_plus
from datetime import datetime
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

# ----------------- config -----------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

APPID = 730
CURRENCY = 1
DELAY = 1.5

# keywords for the target skin
KEYWORDS = ["awp", "duality"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36 (compatible; cs2-price-checker/1.0)"
}

# ----------------- MongoDB -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
names_col = db["all_item_names"]
current_prices_col = db["current_prices"]

# clean current_prices for testing
current_prices_col.delete_many({})
print("Cleared current_prices collection for fresh run.")

# ----------------- helpers -----------------
def build_priceoverview_url(item_name):
    encoded = quote_plus(item_name)
    return ("https://steamcommunity.com/market/priceoverview/"
            f"?appid={APPID}&currency={CURRENCY}&market_hash_name={encoded}&format=json")

def price_to_float(price_str):
    if not price_str:
        return None
    cleaned = price_str.replace("US$", "").replace("$", "").strip().replace(",", "")
    try:
        return float(cleaned)
    except:
        return None

def find_skin_variants(keywords):
    """
    Find all items in the DB whose keywords array contains all target keywords.
    Returns a list of documents (all variants including condition).
    """
    return list(names_col.find({"keywords": {"$all": keywords}}))

# ----------------- main -----------------
def main():
    skin_variants = find_skin_variants(KEYWORDS)
    if not skin_variants:
        print(f"No skin found containing keywords: {KEYWORDS}")
        return

    print(f"Found {len(skin_variants)} variants for keywords {KEYWORDS}.")

    for variant in skin_variants:
        item_name = variant["raw_name"]
        print("Querying Steam for:", item_name)

        url = build_priceoverview_url(item_name)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print("Error querying Steam:", e)
            continue

        if not data.get("success"):
            print(f"Steam returned success=false for {item_name}, skipping.")
            continue

        lowest_price_display = data.get("lowest_price")
        median_price_display = data.get("median_price")
        lowest_price = price_to_float(lowest_price_display)
        median_price = price_to_float(median_price_display)

        # store the price for this variant
        doc_price = {
            "timestamp": datetime.utcnow(),
            "item_name": item_name,
            "condition": variant.get("condition"),
            "lowest_price_display": lowest_price_display,
            "median_price_display": median_price_display,
            "lowest_price": lowest_price,
            "median_price": median_price,
            "source": "steam_market_priceoverview",
        }

        current_prices_col.update_one(
            {"item_name": item_name},
            {"$set": doc_price},
            upsert=True
        )
        print(f"Stored current price for {item_name}.")
        time.sleep(DELAY)

if __name__ == "__main__":
    main()

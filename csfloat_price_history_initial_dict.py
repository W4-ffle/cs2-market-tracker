# csfloat_price_history_initial_dict.py
# pip install "pymongo[srv]" python-dotenv

import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

# ----------------- MongoDB -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
csfloat_daily_col = db["csfloat_daily_prices"]
csfloat_history_col = db["csfloat_price_history"]

def initial_commit_dict():
    """
    Converts existing csfloat_daily_prices into a historical dict format:
    - daily_prices: { "YYYY-MM-DD": price }
    - daily_quantity: { "YYYY-MM-DD": quantity }
    """
    now = datetime.utcnow()
    today = datetime(now.year, now.month, now.day)
    today_str = today.strftime("%Y-%m-%d")

    print(f"Performing initial commit of CSFloat prices for {today_str}...")

    total_inserted = 0
    all_items = csfloat_daily_col.find({})  # grab all items

    for item in all_items:
        item_id = item.get("item_name")
        if not item_id:
            continue  # skip items without an item_id

        # Extract values
        price = item.get("price")
        quantity = item.get("quantity")
        fetched_at = item.get("fetched_at", now)

        # Build document in dict format
        hist_doc = {
            "item_id": item_id,
            "daily_prices": {today_str: price},
            "daily_quantity": {today_str: quantity},
            "fetched_at": fetched_at
        }

        # Insert into history collection
        csfloat_history_col.insert_one(hist_doc)
        total_inserted += 1

    print(f"Initial commit completed. Total items inserted: {total_inserted}")


if __name__ == "__main__":
    initial_commit_dict()

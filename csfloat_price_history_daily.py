# csfloat_price_history_daily_dict.py
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

def daily_snapshot_dict():
    """
    Updates each item's historical record in csfloat_price_history,
    storing daily prices and quantities in dictionaries keyed by date.
    """
    now = datetime.utcnow()
    today_str = now.strftime("%Y-%m-%d")  # e.g., "2025-10-04"

    print(f"Creating daily snapshot (dict format) for {today_str}...")

    total_updated = 0
    todays_items = csfloat_daily_col.find({"date": datetime(now.year, now.month, now.day)})

    for item in todays_items:
        item_id = item.get("item_id")
        if not item_id:
            continue  # skip items without ID

        # Extract values
        price = item.get("price")
        quantity = item.get("quantity")

        # Upsert document with today's price and quantity
        csfloat_history_col.update_one(
            {"item_id": item_id},
            {
                "$set": {
                    f"daily_prices.{today_str}": price,
                    f"daily_quantity.{today_str}": quantity
                }
            },
            upsert=True
        )
        total_updated += 1

    print(f"Daily snapshot completed. Total items updated: {total_updated}")

if __name__ == "__main__":
    daily_snapshot_dict()

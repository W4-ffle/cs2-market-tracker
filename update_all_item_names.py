# update_all_item_names.py
# pip install "pymongo[srv]" python-dotenv

import os
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

# ----------------- MongoDB -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
csfloat_daily_col = db["csfloat_daily_prices"]
all_item_names_col = db["all_item_names"]

def update_all_item_names():
    """
    Adds new items from csfloat_daily_prices to all_item_names with a unique item_id.
    Skips any items that already exist.
    """
    today = datetime.utcnow().date()
    total_added = 0

    # Get existing item_ids / market_hash_names
    existing_names = set(doc["market_hash_name"] for doc in all_item_names_col.find({}, {"market_hash_name": 1}))

    # Fetch items from today's CSFloat snapshot
    daily_items = csfloat_daily_col.find({"date": datetime(today.year, today.month, today.day)})

    for item in daily_items:
        market_name = item.get("market_hash_name")
        if not market_name:
            continue

        if market_name in existing_names:
            continue  # already in all_item_names

        # Create new entry
        new_doc = {
            "csfloat_id": item.get("item_name"),  # must exist from csfloat_daily_prices
            "market_hash_name": market_name,
            "keywords": [word.lower() for word in market_name.replace("|", " ").split()],  # for search
            "created_at": datetime.utcnow()
        }

        all_item_names_col.insert_one(new_doc)
        total_added += 1
        existing_names.add(market_name)  # prevent duplicates in same run

    print(f"All item names updated. Total new items added: {total_added}")


if __name__ == "__main__":
    update_all_item_names()

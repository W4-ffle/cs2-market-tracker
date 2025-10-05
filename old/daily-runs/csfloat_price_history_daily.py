# csfloat_price_history_daily_dict.py
# pip install "pymongo[srv]" python-dotenv

import os
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import re

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

# ----------------- MongoDB -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]

daily_col   = db["csfloat_daily_prices"]     # source snapshots for the day
history_col = db["csfloat_price_history"]    # 1 doc per item with daily dicts
names_col   = db["all_item_names"]           # canonical list used for search

def _keywords(name: str):
    """Basic tokenization for search (keeps letters/numbers and common CS2 symbols)."""
    name = name or ""
    cleaned = re.sub(r"[^\w\*\|\-/\(\)\[\]\s]", " ", name)
    toks = re.split(r"[ \|\-/\(\)\[\]_]+", cleaned.lower())
    return [t for t in toks if t]

def daily_snapshot_dict():
    """
    For each doc in today's csfloat_daily_prices:
      - Use item_name as stable key 'csfloat_id'
      - Ensure an entry exists in all_item_names (upsert)
      - Upsert into csfloat_price_history using csfloat_id, setting:
            daily_prices.<YYYY-MM-DD>   = price
            daily_quantity.<YYYY-MM-DD> = quantity
    """
    now = datetime.utcnow()
    today_dt   = datetime(now.year, now.month, now.day)   # midnight UTC
    today_str  = today_dt.strftime("%Y-%m-%d")

    print(f"Creating daily snapshot (dict format) for {today_str}...")

    # Pull only today's rows (as per your schema screenshot)
    cursor = daily_col.find({"date": today_dt}, {
        "item_name": 1, "price": 1, "quantity": 1, "fetched_at": 1
    })

    # Optional: batch operations for speed
    name_upserts = []
    history_upserts = []
    count = 0

    for doc in cursor:
        csfloat_id = doc.get("item_name")  # STABLE KEY (string)
        if not csfloat_id:
            continue

        price    = doc.get("price")
        quantity = doc.get("quantity")
        fetched  = doc.get("fetched_at", now)

        # 1) Ensure all_item_names contains this item (upsert by csfloat_id)
        name_upserts.append(
            UpdateOne(
                {"csfloat_id": csfloat_id},
                {"$setOnInsert": {
                    "csfloat_id": csfloat_id,
                    "raw_name": csfloat_id,
                    "keywords": _keywords(csfloat_id),
                    "created_at": now
                },
                 "$set": {"last_seen": now}},
                upsert=True
            )
        )

        # 2) Upsert into history (1 doc per item), append today's values
        history_upserts.append(
            UpdateOne(
                {"csfloat_id": csfloat_id},
                {"$set": {
                    f"daily_prices.{today_str}":  price,
                    f"daily_quantity.{today_str}": quantity,
                    "last_fetched_at": fetched
                }},
                upsert=True
            )
        )
        count += 1

    # Execute batched writes (fewer round trips)
    if name_upserts:
        names_col.bulk_write(name_upserts, ordered=False)
    if history_upserts:
        history_col.bulk_write(history_upserts, ordered=False)

    print(f"Daily snapshot completed. Total items updated: {count}")

if __name__ == "__main__":
    daily_snapshot_dict()

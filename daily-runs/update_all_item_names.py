# update_all_item_names.py
# pip install "pymongo[srv]" python-dotenv

import os
import re
from datetime import datetime
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
daily_col = db["csfloat_daily_prices"]
names_col = db["all_item_names"]

def _keywords(name: str):
    """Lowercase tokens for search; keep common CS2 symbols like | * / - () []."""
    name = name or ""
    cleaned = re.sub(r"[^\w\*\|\-/\(\)\[\]\s]", " ", name)
    toks = re.split(r"[ \|\-/\(\)\[\]_]+", cleaned.lower())
    return [t for t in toks if t]

def update_all_item_names():
    """
    Upsert all items seen in today's csfloat_daily_prices into all_item_names.
    Key = csfloat_id (which equals item_name).
    """
    now = datetime.utcnow()
    today_dt = datetime(now.year, now.month, now.day)  # midnight UTC
    print(f"Upserting items from daily snapshot for {today_dt.date()}...")

    # Pull only today's items; project only what we need.
    cursor = daily_col.find({"date": today_dt}, {"item_name": 1})

    ops = []
    seen = 0
    for doc in cursor:
        csfloat_id = doc.get("item_name")
        if not csfloat_id:
            continue
        seen += 1
        ops.append(
            UpdateOne(
                {"csfloat_id": csfloat_id},
                {
                    "$setOnInsert": {
                        "csfloat_id": csfloat_id,
                        "raw_name": csfloat_id,
                        "keywords": _keywords(csfloat_id),
                        "created_at": now,
                    },
                    "$set": {"last_seen": now},
                },
                upsert=True,
            )
        )

        # Flush periodically to keep memory low on huge sets
        if len(ops) >= 5000:
            names_col.bulk_write(ops, ordered=False)
            ops.clear()

    if ops:
        names_col.bulk_write(ops, ordered=False)

    print(f"Processed {seen} daily items. all_item_names upsert complete.")

if __name__ == "__main__":
    update_all_item_names()

# csfloat_price_history_initial_dict.py
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

# ----------------- Mongo -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
daily_col   = db["csfloat_daily_prices"]      # source data (can contain many docs)
history_col = db["csfloat_price_history"]     # 1 doc per item (with daily dicts)
names_col   = db["all_item_names"]            # search index / canonical list

def _keywords(name: str):
    name = name or ""
    cleaned = re.sub(r"[^\w\*\|\-/\(\)\[\]\s]", " ", name)
    toks = re.split(r"[ \|\-/\(\)\[\]_]+", cleaned.lower())
    return [t for t in toks if t]

def seed_history_from_daily():
    """
    One-time initializer:
      - Reads EVERY doc in csfloat_daily_prices (all dates).
      - Uses item_name as stable key `csfloat_id`.
      - Upserts all_item_names (so search table has every item).
      - Builds csfloat_price_history with:
           daily_prices.<YYYY-MM-DD>   = price
           daily_quantity.<YYYY-MM-DD> = quantity
        (one document per item, dictionaries grow per date)
    """
    now = datetime.utcnow()
    print("Seeding compact history from csfloat_daily_prices ...")

    # Stream all docs (project only what we need)
    cursor = daily_col.find({}, {
        "item_name": 1, "price": 1, "quantity": 1, "date": 1, "fetched_at": 1
    })

    name_upserts: list[UpdateOne] = []
    hist_upserts: list[UpdateOne] = []
    count = 0

    for doc in cursor:
        csfloat_id = doc.get("item_name")  # STABLE key
        if not csfloat_id:
            continue

        # Date key: prefer midnight 'date', else use fetched_at’s date, else today
        dt = doc.get("date") or doc.get("fetched_at") or now
        if not isinstance(dt, datetime):
            dt = now
        date_key = dt.strftime("%Y-%m-%d")

        price    = doc.get("price")
        quantity = doc.get("quantity")
        fetched  = doc.get("fetched_at", now)

        # 1) Ensure all_item_names has this item (upsert by csfloat_id)
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

        # 2) Upsert history doc for this item, attach date-keyed values
        # Using $set means reruns will safely overwrite same-day values.
        hist_upserts.append(
            UpdateOne(
                {"csfloat_id": csfloat_id},
                {"$set": {
                    f"daily_prices.{date_key}":  price,
                    f"daily_quantity.{date_key}": quantity,
                    "last_fetched_at": fetched
                }},
                upsert=True
            )
        )
        count += 1

        # Optional: flush periodically to keep memory low
        if len(hist_upserts) >= 5_000:
            if name_upserts:
                names_col.bulk_write(name_upserts, ordered=False)
                name_upserts.clear()
            history_col.bulk_write(hist_upserts, ordered=False)
            hist_upserts.clear()

    # Final flush
    if name_upserts:
        names_col.bulk_write(name_upserts, ordered=False)
    if hist_upserts:
        history_col.bulk_write(hist_upserts, ordered=False)

    print(f"Seeding complete. Processed {count} daily rows.")

if __name__ == "__main__":
    seed_history_from_daily()

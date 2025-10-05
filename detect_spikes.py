# pip install "pymongo[srv]" python-dotenv
import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
HIST_COL = os.getenv("HISTORY_COLLECTION")
ALERTS_COL = os.getenv("ALERTS_COLLECTION")

# thresholds (override in .env or GH secrets if you like)
PRICE_PCT = float(os.getenv("PRICE_PCT"))
QTY_PCT   = float(os.getenv("QTY_PCT"))
MIN_QTY_DELTA = int(os.getenv("MIN_QTY_DELTA"))

def iso_hour(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:00")

def main():
    if not MONGO_URI:
        raise SystemExit("Set MONGO_URI")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    db = client[DB_NAME]
    hist = db[HIST_COL]
    alerts = db[ALERTS_COL]

    # Determine hour keys
    now = datetime.now(timezone.utc)
    cur_hour = now.replace(minute=0, second=0, microsecond=0)
    prev_hour = cur_hour - timedelta(hours=1)

    k_cur  = iso_hour(cur_hour)
    k_prev = iso_hour(prev_hour)

    # Projection only what we need
    proj = {
        "_id": 0,
        "csfloat_id": 1,
        f"hourly_prices.{k_prev}": 1,
        f"hourly_prices.{k_cur}": 1,
        f"hourly_quantity.{k_prev}": 1,
        f"hourly_quantity.{k_cur}": 1,
    }

    ops = []
    count = 0
    for doc in hist.find({}, proj, no_cursor_timeout=True):
        item = doc["csfloat_id"]
        p_prev = (((doc.get("hourly_prices") or {}).get(k_prev)) if doc.get("hourly_prices") else None)
        p_cur  = (((doc.get("hourly_prices") or {}).get(k_cur)) if doc.get("hourly_prices") else None)
        q_prev = (((doc.get("hourly_quantity") or {}).get(k_prev)) if doc.get("hourly_quantity") else None)
        q_cur  = (((doc.get("hourly_quantity") or {}).get(k_cur)) if doc.get("hourly_quantity") else None)

        changes = []
        # Price jump
        if isinstance(p_prev, (int, float)) and isinstance(p_cur, (int, float)) and p_prev > 0:
            price_delta = p_cur - p_prev
            price_pct = price_delta / p_prev
            if abs(price_pct) >= PRICE_PCT:
                changes.append({
                    "field": "price",
                    "prev": p_prev, "cur": p_cur,
                    "abs_change": price_delta,
                    "pct_change": price_pct
                })

        # Quantity jump
        if isinstance(q_prev, int) and isinstance(q_cur, int) and max(q_prev, q_cur) > 0:
            qty_delta = q_cur - q_prev
            # protect against tiny noise
            if abs(qty_delta) >= MIN_QTY_DELTA:
                base = q_prev if q_prev != 0 else max(q_cur, 1)
                qty_pct = qty_delta / base
                if abs(qty_pct) >= QTY_PCT:
                    changes.append({
                        "field": "quantity",
                        "prev": q_prev, "cur": q_cur,
                        "abs_change": qty_delta,
                        "pct_change": qty_pct
                    })

        if not changes:
            continue

        ops.append(UpdateOne(
            {"csfloat_id": item, "hour": cur_hour},
            {"$set": {
                "csfloat_id": item,
                "hour": cur_hour,
                "changes": changes,
                "detected_at": now
            }},
            upsert=True
        ))
        count += 1

        if len(ops) >= 1000:
            alerts.bulk_write(ops, ordered=False)
            ops.clear()

    if ops:
        alerts.bulk_write(ops, ordered=False)

    print(f"✅ Spike detection complete. Alerts upserted: {count}")

if __name__ == "__main__":
    main()

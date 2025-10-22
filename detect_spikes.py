# detect_spikes.py
# Fast, Atlas-friendly spike detector: diffs prev hour -> current hour per item via projection.
# pip install "pymongo[srv]" python-dotenv

import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
HIST_COLL = os.getenv("HISTORY_COLLECTION")
ALERTS_COLL = os.getenv("ALERTS_COLLECTION")

# thresholds (tune to your needs)
PRICE_PCT = float(os.getenv("PRICE_PCT", "0.15"))       # 15% price move
QTY_PCT   = float(os.getenv("QTY_PCT",   "0.25"))       # 25% qty move
MIN_QTY_DELTA = float(os.getenv("MIN_QTY_DELTA", "3"))  # at least ±3 units change

# Safety limits
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "10000"))  # per-op timeout
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "500"))

if not MONGO_URI:
    raise SystemExit("Set MONGO_URI")

def hour_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:00")

def pct_change(prev, cur):
    if prev is None or cur is None:
        return None
    if prev == 0:
        # avoid inf; treat as None so we can ignore or handle separately
        return None
    return (cur - prev) / prev

def main():
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    prev = now - timedelta(hours=1)
    hk_now = hour_key(now)
    hk_prev = hour_key(prev)

    print(f"[{datetime.now(timezone.utc).isoformat()}] Detecting spikes for {hk_prev} -> {hk_now}")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=MONGO_TIMEOUT_MS,
        connectTimeoutMS=5000,
        maxPoolSize=50,
        retryWrites=False,
    )
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed:", e)
        return

    db = client[DB_NAME]
    hist = db[HIST_COLL]
    alerts = db[ALERTS_COLL]

    # Only scan docs updated recently (optional but recommended on large sets).
    # If you always write last_fetched_at in your hourly updater, uncomment this:
    # since = now - timedelta(hours=6)
    # base_query = { "last_fetched_at": {"$gte": since} }
    base_query = {}  # or keep empty to scan all

    projection = {
        "_id": 0,
        "csfloat_id": 1,
        f"hourly_prices.{hk_prev}": 1,
        f"hourly_prices.{hk_now}": 1,
        f"hourly_quantity.{hk_prev}": 1,
        f"hourly_quantity.{hk_now}": 1,
    }

    ops = []
    count_scanned = 0
    count_alerts = 0

    cursor = hist.find(base_query, projection, batch_size=BATCH_SIZE, no_cursor_timeout=False)
    try:
        for doc in cursor:
            count_scanned += 1
            cid = doc.get("csfloat_id")

            # Pull values (may be missing)
            p_prev = doc.get("hourly_prices", {}).get(hk_prev)
            p_now  = doc.get("hourly_prices", {}).get(hk_now)
            q_prev = doc.get("hourly_quantity", {}).get(hk_prev)
            q_now  = doc.get("hourly_quantity", {}).get(hk_now)

            # --- PRICE spike ---
            p_pct = pct_change(p_prev, p_now)
            if p_pct is not None and abs(p_pct) >= PRICE_PCT:
                changes = [{
                    "field": "price",
                    "prev": p_prev, "cur": p_now,
                    "abs_change": (None if (p_prev is None or p_now is None) else p_now - p_prev),
                    "pct_change": p_pct,
                }]
                ops.append(UpdateOne(
                    {"csfloat_id": cid, "hour": now},
                    {"$set": {"csfloat_id": cid, "hour": now}, "$push": {"changes": {"$each": changes}}},
                    upsert=True
                ))
                count_alerts += 1

            # --- QUANTITY spike ---
            if q_prev is not None and q_now is not None:
                q_abs = q_now - q_prev
                q_pct = pct_change(q_prev, q_now)
                if (abs(q_abs) >= MIN_QTY_DELTA) and (q_pct is not None and abs(q_pct) >= QTY_PCT):
                    changes = [{
                        "field": "quantity",
                        "prev": q_prev, "cur": q_now,
                        "abs_change": q_abs,
                        "pct_change": q_pct,
                    }]
                    ops.append(UpdateOne(
                        {"csfloat_id": cid, "hour": now},
                        {"$set": {"csfloat_id": cid, "hour": now}, "$push": {"changes": {"$each": changes}}},
                        upsert=True
                    ))
                    count_alerts += 1

            if len(ops) >= 1000:
                try:
                    alerts.bulk_write(ops, ordered=False)
                except PyMongoError as e:
                    print("⚠️ bulk_write error:", e)
                ops.clear()

        if ops:
            try:
                alerts.bulk_write(ops, ordered=False)
            except PyMongoError as e:
                print("⚠️ bulk_write error:", e)

    finally:
        cursor.close()
        client.close()

    print(f"✅ scanned={count_scanned}, new_alerts={count_alerts}, hour={hk_now}")

if __name__ == "__main__":
    main()

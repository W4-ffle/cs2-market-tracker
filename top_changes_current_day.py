# top_changes_current_day.py
# Hourly job: compute top-50 changes for the current UTC day and write to 3 collections.
# Falls back to yesterday if today has zero alerts.
# Collections:
#   - top_changes_price_daily
#   - top_changes_quantity_daily
#   - top_changes_any_daily

import os
from datetime import datetime, timedelta, timezone
from typing import Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION")

COLL_PRICE = os.getenv("TOP_PRICE_COLLECTION")
COLL_QUANT = os.getenv("TOP_QUANTITY_COLLECTION")
COLL_ANY   = os.getenv("TOP_ANY_COLLECTION")

TOP_LIMIT = int(os.getenv("TOP_LIMIT"))
METRIC    = os.getenv("TOP_METRIC")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE")          # YYYY-MM-DD (optional manual override)
DISABLE_FALLBACK = os.getenv("DISABLE_FALLBACK", "0") in ("1", "true", "True")

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI")

def day_window(date_utc: datetime) -> Tuple[datetime, datetime]:
    start = date_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)

def today_utc_start() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def parse_utc_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def pipeline_for_day(start: datetime, end: datetime, field_mode: str, metric: str, limit: int):
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}
    pipeline = [
        {"$addFields": {
            "hour_dt": {
                "$cond": [
                    {"$eq": [{"$type": "$hour"}, "string"]},
                    {"$toDate": "$hour"},
                    "$hour"
                ]
            }
        }},
        {"$match": {"hour_dt": {"$gte": start, "$lt": end}}},
        {"$unwind": "$changes"},
    ]
    if field_mode in ("price", "quantity"):
        pipeline.append({"$match": {"changes.field": field_mode}})
    pipeline += [
        {"$addFields": {"score": score_expr}},
        {"$sort": {"score": -1, "hour_dt": -1}},
        {"$group": {
            "_id": {"csfloat_id": "$csfloat_id", "field": "$changes.field"},
            "csfloat_id": {"$first": "$csfloat_id"},
            "field": {"$first": "$changes.field"},
            "hour": {"$first": "$hour_dt"},
            "prev": {"$first": "$changes.prev"},
            "cur": {"$first": "$changes.cur"},
            "abs_change": {"$first": "$changes.abs_change"},
            "pct_change": {"$first": "$changes.pct_change"},
            "score": {"$first": "$score"},
        }},
    ]
    if field_mode == "any":
        pipeline += [
            {"$sort": {"score": -1, "hour": -1}},
            {"$group": {
                "_id": "$csfloat_id",
                "csfloat_id": {"$first": "$csfloat_id"},
                "field": {"$first": "$field"},
                "hour": {"$first": "$hour"},
                "prev": {"$first": "$prev"},
                "cur": {"$first": "$cur"},
                "abs_change": {"$first": "$abs_change"},
                "pct_change": {"$first": "$pct_change"},
                "score": {"$first": "$score"},
            }},
        ]
    pipeline += [
        {"$sort": {"score": -1, "hour": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0}}
    ]
    return pipeline

def count_alerts_in_window(alerts, start: datetime, end: datetime) -> int:
    return alerts.count_documents({"hour": {"$gte": start, "$lt": end}})

def main():
    # Determine target day (override > today; fallback to yesterday if empty and not disabled)
    if SNAPSHOT_DATE:
        target_day = parse_utc_date(SNAPSHOT_DATE)
        mode = f"manual({SNAPSHOT_DATE})"
    else:
        target_day = today_utc_start()
        mode = "today"

    start, end = day_window(target_day)
    print(f"🕒 Computing TOP {TOP_LIMIT} for {mode} UTC day: {start.date()} (window {start.isoformat()} → {end.isoformat()})")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed (auth/network):", e)
        return

    db = client[DB_NAME]
    alerts = db[ALERTS_COLLECTION]
    coll_price = db[COLL_PRICE]
    coll_quant = db[COLL_QUANT]
    coll_any = db[COLL_ANY]

    # Optional fallback if no alerts for the chosen day and not manually overridden
    alert_count = count_alerts_in_window(alerts, start, end)
    if alert_count == 0 and not SNAPSHOT_DATE and not DISABLE_FALLBACK:
        # try yesterday
        yday = target_day - timedelta(days=1)
        ystart, yend = day_window(yday)
        ycount = count_alerts_in_window(alerts, ystart, yend)
        if ycount > 0:
            print(f"ℹ️ No alerts today yet; falling back to yesterday: {ystart.date()} ({ycount} alerts)")
            start, end = ystart, yend
        else:
            print("ℹ️ No alerts found for today or yesterday. Writing empty snapshots for today.")
            # keep start/end as today; proceed to write empty (so consumers still see a doc)

    # Ensure one doc per day per collection
    for c in (coll_price, coll_quant, coll_any):
        try:
            c.create_index([("snapshot_date", 1)], unique=True)
        except PyMongoError as e:
            print("⚠️ Index creation warning:", e)

    now = datetime.now(timezone.utc)
    common_meta = {
        "snapshot_date": start,                  # anchor = chosen day 00:00Z
        "window": {"start": start, "end": end},
        "metric": METRIC,
        "limit": TOP_LIMIT,
        "generated_at": now,
    }

    def run_into(field_mode: str, target):
        rows = list(alerts.aggregate(pipeline_for_day(start, end, field_mode, METRIC, TOP_LIMIT),
                                     allowDiskUse=True, maxTimeMS=180000))
        target.replace_one({"snapshot_date": start}, {**common_meta, "field_mode": field_mode, "items": rows}, upsert=True)
        print(f"✅ Wrote {len(rows):>2} rows -> {target.name} ({field_mode})")

    run_into("price", coll_price)
    run_into("quantity", coll_quant)
    run_into("any", coll_any)

    client.close()
    print("🏁 Done.")

if __name__ == "__main__":
    main()

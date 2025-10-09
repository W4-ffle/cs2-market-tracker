# top_changes_snapshot.py
# Create daily snapshots of top 50 changes from alerts into 3 collections
# pip install "pymongo[srv]" python-dotenv

import os
import argparse
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "cs2_market")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

COLL_PRICE   = os.getenv("TOP_PRICE_COLLECTION",   "top_changes_price_daily")
COLL_QUANT   = os.getenv("TOP_QUANTITY_COLLECTION","top_changes_quantity_daily")
COLL_ANY     = os.getenv("TOP_ANY_COLLECTION",     "top_changes_any_daily")

LIMIT = int(os.getenv("TOP_LIMIT", "50"))
METRIC = os.getenv("TOP_METRIC", "pct")  # "pct" (default) or "abs"

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI")

def parse_args():
    p = argparse.ArgumentParser(description="Create daily top-changes snapshots")
    p.add_argument("--date", help="Single UTC day YYYY-MM-DD (e.g., 2025-10-05)")
    p.add_argument("--from", dest="date_from", help="Start UTC day YYYY-MM-DD (inclusive)")
    p.add_argument("--to", dest="date_to", help="End UTC day YYYY-MM-DD (inclusive)")
    return p.parse_args()

def day_start_utc(d: datetime) -> datetime:
    return d.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def parse_day(s: str) -> datetime:
    # interpret as UTC date start
    dt = datetime.strptime(s, "%Y-%m-%d")
    return day_start_utc(dt)

def day_window_utc(day0: datetime) -> tuple[datetime, datetime]:
    start = day_start_utc(day0)
    end = start + timedelta(days=1)
    return start, end

def make_pipeline(start: datetime, end: datetime, field_mode: str, metric: str, limit: int):
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}
    pipeline = [
        {"$match": {"hour": {"$gte": start, "$lt": end}}},
        {"$unwind": "$changes"},
    ]
    if field_mode in ("price", "quantity"):
        pipeline.append({"$match": {"changes.field": field_mode}})

    pipeline += [
        {"$addFields": {"score": score_expr}},
        {"$sort": {"score": -1, "hour": -1}},
        {"$group": {
            "_id": {"csfloat_id": "$csfloat_id", "field": "$changes.field"},
            "csfloat_id": {"$first": "$csfloat_id"},
            "field": {"$first": "$changes.field"},
            "hour": {"$first": "$hour"},
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

def snapshot_for_day(client: MongoClient, snapshot_date: datetime):
    db = client[DB_NAME]
    alerts = db[ALERTS_COLLECTION]
    price_coll = db[COLL_PRICE]
    qty_coll   = db[COLL_QUANT]
    any_coll   = db[COLL_ANY]

    # one doc per day per collection
    for coll in (price_coll, qty_coll, any_coll):
        try:
            coll.create_index([("snapshot_date", 1)], unique=True)
        except PyMongoError as e:
            print("⚠️ Index creation warning:", e)

    start, end = day_window_utc(snapshot_date)

    print(f"📅 Building snapshots for {start.date()} (UTC)")
    common_meta = {
        "snapshot_date": start,
        "window": {"start": start, "end": end},
        "metric": METRIC,
        "limit": LIMIT,
        "generated_at": datetime.now(timezone.utc),
    }

    def run_and_write(field_mode: str, target_coll):
        pipeline = make_pipeline(start, end, field_mode, METRIC, LIMIT)
        rows = list(alerts.aggregate(pipeline, allowDiskUse=True, maxTimeMS=180000))
        doc = {**common_meta, "field_mode": field_mode, "items": rows}
        target_coll.replace_one({"snapshot_date": start}, doc, upsert=True)
        print(f"✅ {field_mode:<8} -> {len(rows)} items written to {target_coll.name}")

    run_and_write("price",    price_coll)
    run_and_write("quantity", qty_coll)
    run_and_write("any",      any_coll)

def main():
    args = parse_args()
    now_utc = day_start_utc(datetime.now(timezone.utc))

    if args.date and (args.date_from or args.date_to):
        raise SystemExit("Use either --date or --from/--to, not both.")

    if args.date:
        days = [parse_day(args.date)]
    elif args.date_from and args.date_to:
        start_day = parse_day(args.date_from)
        end_day = parse_day(args.date_to)
        if end_day < start_day:
            raise SystemExit("--to must be >= --from")
        # inclusive range
        n = (end_day - start_day).days + 1
        days = [start_day + timedelta(days=i) for i in range(n)]
    else:
        # default: yesterday UTC
        days = [now_utc - timedelta(days=1)]

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed (auth/network):", e)
        return

    for d in days:
        snapshot_for_day(client, d)

    client.close()

if __name__ == "__main__":
    main()

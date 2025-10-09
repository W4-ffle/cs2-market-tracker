# top_changes_current_day.py
# Hourly job: compute top-50 changes for the current UTC day and write to 3 collections.
# Collections written:
#   - top_changes_price_daily
#   - top_changes_quantity_daily
#   - top_changes_any_daily
#
# Assumes alerts documents like:
# {
#   csfloat_id: "AK-47 | Leet Museo (Minimal Wear)",
#   hour: ISODate("2025-10-08T01:00:00Z"),
#   changes: [
#     { field: "price",    prev: 26076, cur: 31404, abs_change: 5328, pct_change: 0.2043 },
#     { field: "quantity", prev: 15,    cur: 13,    abs_change: -2,   pct_change: -0.1333 }
#   ]
# }

import os
from datetime import datetime, timedelta, timezone
from typing import Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

# ---------- ENV ----------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION")

COLL_PRICE = os.getenv("TOP_PRICE_COLLECTION")
COLL_QUANT = os.getenv("TOP_QUANTITY_COLLECTION")
COLL_ANY   = os.getenv("TOP_ANY_COLLECTION")

TOP_LIMIT = int(os.getenv("TOP_LIMIT"))
METRIC = os.getenv("TOP_METRIC")

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI (in .env or repo secrets)")

# ---------- TIME HELPERS ----------
def today_utc_window() -> Tuple[datetime, datetime]:
    """Return (start, end) for the current UTC day."""
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

# ---------- PIPELINE BUILDER ----------
def pipeline_for_day(start: datetime, end: datetime, field_mode: str, metric: str, limit: int):
    """
    field_mode: "price" | "quantity" | "any"
    metric: "pct" | "abs"
    """
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}

    pipeline = [
        # Normalize "hour" to a Date (supports both Date and string storage)
        {"$addFields": {
            "hour_dt": {
                "$cond": [
                    {"$eq": [{"$type": "$hour"}, "string"]},
                    {"$toDate": "$hour"},
                    "$hour"
                ]
            }
        }},
        # Restrict to today's UTC window
        {"$match": {"hour_dt": {"$gte": start, "$lt": end}}},
        # Each alert may have both "price" and "quantity" changes
        {"$unwind": "$changes"},
    ]

    if field_mode in ("price", "quantity"):
        pipeline.append({"$match": {"changes.field": field_mode}})

    pipeline += [
        # Score we rank by (abs pct or abs raw)
        {"$addFields": {"score": score_expr}},
        # Take strongest first; tie-breaker = most recent hour
        {"$sort": {"score": -1, "hour_dt": -1}},

        # Keep only the strongest change per (item, field)
        {"$group": {
            "_id": {"csfloat_id": "$csfloat_id", "field": "$changes.field"},
            "csfloat_id": {"$first": "$csfloat_id"},
            "field": {"$first": "$changes.field"},
            "hour": {"$first": "$hour_dt"},
            "prev": {"$first": "$changes.prev"},
            "cur": {"$first": "$changes.cur"},
            "abs_change": {"$first": "$changes.abs_change"},
            "pct_change": {"$first": "$changes.pct_change"},
            "score": {"$first": "$score"}
        }},
    ]

    if field_mode == "any":
        # From the two fields, keep a single best record per item
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
                "score": {"$first": "$score"}
            }},
        ]

    pipeline += [
        {"$sort": {"score": -1, "hour": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0}}
    ]

    return pipeline

# ---------- MAIN ----------
def main():
    start, end = today_utc_window()
    print(f"🕒 Building TOP {TOP_LIMIT} for current UTC day: {start.date()}")

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

    # Ensure one document per day per collection
    for c in (coll_price, coll_quant, coll_any):
        try:
            c.create_index([("snapshot_date", 1)], unique=True)
        except PyMongoError as e:
            print("⚠️ Index creation warning:", e)

    now = datetime.now(timezone.utc)

    common_meta = {
        "snapshot_date": start,              # anchor at 00:00Z today
        "window": {"start": start, "end": end},
        "metric": METRIC,
        "limit": TOP_LIMIT,
        "generated_at": now,
    }

    def run_into(field_mode: str, target):
        pipeline = pipeline_for_day(start, end, field_mode, METRIC, TOP_LIMIT)
        rows = list(alerts.aggregate(pipeline, allowDiskUse=True, maxTimeMS=180000))
        doc = {**common_meta, "field_mode": field_mode, "items": rows}
        target.replace_one({"snapshot_date": start}, doc, upsert=True)
        print(f"✅ Wrote {len(rows):>2} rows -> {target.name} ({field_mode})")

    run_into("price", coll_price)
    run_into("quantity", coll_quant)
    run_into("any", coll_any)

    client.close()
    print("🏁 Done.")

if __name__ == "__main__":
    main()

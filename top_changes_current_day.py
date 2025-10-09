# top_changes_current_day.py
# Hourly job: compute top-50 changes per category (Stickers, Weapons, Cases) for the current UTC day.
# Falls back to yesterday if today has zero alerts.
# Writes to:
#   - top_stickers_daily
#   - top_weapons_daily
#   - top_cases_daily

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

TOP_LIMIT = int(os.getenv("TOP_LIMIT", "50"))
METRIC = os.getenv("TOP_METRIC", "pct")
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE")
DISABLE_FALLBACK = os.getenv("DISABLE_FALLBACK", "0") in ("1", "true", "True")

# Collections for categories
COLL_STICKERS = os.getenv("TOP_STICKERS_COLLECTION", "top_stickers_daily")
COLL_WEAPONS = os.getenv("TOP_WEAPONS_COLLECTION", "top_weapons_daily")
COLL_CASES   = os.getenv("TOP_CASES_COLLECTION", "top_cases_daily")

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI in environment or secrets")


def day_window(date_utc: datetime) -> Tuple[datetime, datetime]:
    start = date_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def today_utc_start() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def parse_utc_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def classify_item(csfloat_id: str) -> str:
    """Rudimentary item classifier based on name."""
    name = csfloat_id.lower()
    if "sticker" in name:
        return "sticker"
    if "case" in name or "capsule" in name:
        return "case"
    return "weapon"


def pipeline_for_day(start: datetime, end: datetime, field_mode: str, metric: str, limit: int):
    """Aggregation to get top changes for the given field_mode."""
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
        {"$project": {"_id": 0}},
    ]

    return pipeline


def count_alerts_in_window(alerts, start: datetime, end: datetime) -> int:
    return alerts.count_documents({"hour": {"$gte": start, "$lt": end}})


def main():
    # Determine target day (override > today; fallback to yesterday if empty)
    if SNAPSHOT_DATE:
        target_day = parse_utc_date(SNAPSHOT_DATE)
        mode = f"manual({SNAPSHOT_DATE})"
    else:
        target_day = today_utc_start()
        mode = "today"

    start, end = day_window(target_day)
    print(f"🕒 Computing TOP {TOP_LIMIT} per category for {mode} UTC day {start.date()}")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed:", e)
        return

    db = client[DB_NAME]
    alerts = db[ALERTS_COLLECTION]

    collections = {
        "sticker": db[COLL_STICKERS],
        "weapon": db[COLL_WEAPONS],
        "case": db[COLL_CASES],
    }

    # Fallback check
    alert_count = count_alerts_in_window(alerts, start, end)
    if alert_count == 0 and not SNAPSHOT_DATE and not DISABLE_FALLBACK:
        yday = target_day - timedelta(days=1)
        ystart, yend = day_window(yday)
        ycount = count_alerts_in_window(alerts, ystart, yend)
        if ycount > 0:
            print(f"ℹ️ No alerts today; falling back to yesterday {ystart.date()} ({ycount} alerts)")
            start, end = ystart, yend
        else:
            print("ℹ️ No alerts found for today or yesterday — writing empty docs.")
            # Continue anyway

    # Index setup
    for c in collections.values():
        try:
            c.create_index([("snapshot_date", 1)], unique=True)
        except PyMongoError:
            pass

    now = datetime.now(timezone.utc)
    common_meta = {
        "snapshot_date": start,
        "window": {"start": start, "end": end},
        "metric": METRIC,
        "limit": TOP_LIMIT,
        "generated_at": now,
    }

    # Fetch once and categorize
    all_items = list(alerts.aggregate(pipeline_for_day(start, end, "any", METRIC, 5000)))
    categorized = {"sticker": [], "weapon": [], "case": []}
    for item in all_items:
        cat = classify_item(item["csfloat_id"])
        categorized[cat].append(item)

    # Save each category
    for cat, col in collections.items():
        # Get top by category
        sorted_items = sorted(categorized[cat], key=lambda x: x["score"], reverse=True)[:TOP_LIMIT]
        doc = {**common_meta, "category": cat, "items": sorted_items}
        col.replace_one({"snapshot_date": start}, doc, upsert=True)
        print(f"✅ Wrote {len(sorted_items):>2} rows → {col.name}")

    client.close()
    print("🏁 Done.")


if __name__ == "__main__":
    main()

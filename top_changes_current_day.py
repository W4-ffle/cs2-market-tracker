# top_changes_current_day.py
# Hourly job: compute top-50 changes per category (Stickers, Weapons, Cases) for the current UTC day.
# Falls back to yesterday if today has zero alerts.
# Each category doc includes:
#   - top_price:    price-only top N
#   - top_quantity: quantity-only top N
#   - top_any:      overall top N (best of price or quantity per item)

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Tuple, Dict, List

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dotenv import load_dotenv

load_dotenv()

# ---------- ENVIRONMENT ----------
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION", "alerts")

TOP_LIMIT = int(os.getenv("TOP_LIMIT", "50"))
METRIC = os.getenv("TOP_METRIC", "pct")  # "pct" or "abs"
SNAPSHOT_DATE = os.getenv("SNAPSHOT_DATE")  # YYYY-MM-DD optional override
DISABLE_FALLBACK = os.getenv("DISABLE_FALLBACK", "0") in ("1", "true", "True")

# Output collections (one doc per day per collection)
COLL_STICKERS = os.getenv("TOP_STICKERS_COLLECTION", "top_stickers_daily")
COLL_WEAPONS  = os.getenv("TOP_WEAPONS_COLLECTION",  "top_weapons_daily")
COLL_CASES    = os.getenv("TOP_CASES_COLLECTION",    "top_cases_daily")

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI in environment or secrets")


# ---------- TIME HELPERS ----------
def day_window(date_utc: datetime) -> Tuple[datetime, datetime]:
    start = date_utc.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)

def today_utc_start() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def parse_utc_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


# ---------- CLASSIFICATION ----------
# Broader container-like keywords (use word boundaries)
_CASE_KEYWORDS = [
    r"\bcase\b",                 # e.g. "Clutch Case", "Fracture Case"
    r"\bcapsule\b",              # e.g. "Sticker Capsule"
    r"\bpackage\b",              # e.g. "Souvenir Package"
    r"\bsouvenir package\b",
    r"\bcollection\b",           # e.g. "The Cobblestone Collection"
    r"\bgraffiti box\b",
]

# Exclusions: things that look like "case" but are weapons
_CASE_EXCLUDES = [
    r"\bcase hardened\b",
    r"\bcase-hardened\b",
    r"\bcasehardened\b",
]

def classify_item(csfloat_id: str) -> str:
    """Classify item into sticker / case / weapon using regex rules (avoids 'Case Hardened')."""
    name = (csfloat_id or "").lower()

    # Explicit sticker check
    if "sticker" in name:
        return "sticker"

    # Case-like objects, unless excluded
    if not any(re.search(p, name) for p in _CASE_EXCLUDES):
        if any(re.search(p, name) for p in _CASE_KEYWORDS):
            return "case"

    # Default bucket
    return "weapon"


# ---------- PIPELINES ----------
def base_pipeline_window(start: datetime, end: datetime) -> list:
    """Common stages to normalize hour and filter to the day window, then unwind."""
    return [
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

def pipeline_price(start: datetime, end: datetime, metric: str, limit: int) -> list:
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}
    return [
        *base_pipeline_window(start, end),
        {"$match": {"changes.field": "price"}},
        {"$addFields": {"score": score_expr}},
        {"$sort": {"score": -1, "hour_dt": -1}},
        {"$group": {
            "_id": {"csfloat_id": "$csfloat_id"},
            "csfloat_id": {"$first": "$csfloat_id"},
            "field": {"$first": "$changes.field"},
            "hour": {"$first": "$hour_dt"},
            "prev": {"$first": "$changes.prev"},
            "cur": {"$first": "$changes.cur"},
            "abs_change": {"$first": "$changes.abs_change"},
            "pct_change": {"$first": "$changes.pct_change"},
            "score": {"$first": "$score"},
        }},
        {"$sort": {"score": -1, "hour": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]

def pipeline_quantity(start: datetime, end: datetime, metric: str, limit: int) -> list:
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}
    return [
        *base_pipeline_window(start, end),
        {"$match": {"changes.field": "quantity"}},
        {"$addFields": {"score": score_expr}},
        {"$sort": {"score": -1, "hour_dt": -1}},
        {"$group": {
            "_id": {"csfloat_id": "$csfloat_id"},
            "csfloat_id": {"$first": "$csfloat_id"},
            "field": {"$first": "$changes.field"},
            "hour": {"$first": "$hour_dt"},
            "prev": {"$first": "$changes.prev"},
            "cur": {"$first": "$changes.cur"},
            "abs_change": {"$first": "$changes.abs_change"},
            "pct_change": {"$first": "$changes.pct_change"},
            "score": {"$first": "$score"},
        }},
        {"$sort": {"score": -1, "hour": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]

def pipeline_any(start: datetime, end: datetime, metric: str, limit: int) -> list:
    """Best of price/quantity per item (single record per csfloat_id)."""
    score_expr = {"$abs": "$changes.pct_change"} if metric == "pct" else {"$abs": "$changes.abs_change"}
    return [
        *base_pipeline_window(start, end),
        {"$addFields": {"score": score_expr}},
        {"$sort": {"score": -1, "hour_dt": -1}},
        # First, keep best per (item, field)
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
        # Then keep best overall per item across both fields
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
        {"$sort": {"score": -1, "hour": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]


# ---------- HELPERS ----------
def count_alerts_in_window(alerts, start: datetime, end: datetime) -> int:
    return alerts.count_documents({"hour": {"$gte": start, "$lt": end}})

def split_by_category(rows: List[dict]) -> Dict[str, List[dict]]:
    out = {"sticker": [], "weapon": [], "case": []}
    for r in rows:
        cat = classify_item(r.get("csfloat_id", ""))
        out[cat].append(r)
    return out


# ---------- MAIN ----------
def main():
    # Determine day (manual override > today; optional fallback to yesterday)
    if SNAPSHOT_DATE:
        target_day = parse_utc_date(SNAPSHOT_DATE)
        mode = f"manual({SNAPSHOT_DATE})"
    else:
        target_day = today_utc_start()
        mode = "today"

    start, end = day_window(target_day)
    print(f"🕒 Building TOP {TOP_LIMIT} per category for {mode} UTC day {start.date()}")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed:", e)
        return

    db = client[DB_NAME]
    alerts = db[ALERTS_COLLECTION]

    # Fallback if no alerts for today
    if not SNAPSHOT_DATE and not DISABLE_FALLBACK:
        cnt = count_alerts_in_window(alerts, start, end)
        if cnt == 0:
            ystart, yend = day_window(target_day - timedelta(days=1))
            ycnt = count_alerts_in_window(alerts, ystart, yend)
            if ycnt > 0:
                print(f"ℹ️ No alerts today; falling back to yesterday {ystart.date()} ({ycnt} alerts)")
                start, end = ystart, yend
            else:
                print("ℹ️ No alerts today or yesterday — will still write empty docs.")

    # Output collections
    col_map = {
        "sticker": db[COLL_STICKERS],
        "weapon":  db[COLL_WEAPONS],
        "case":    db[COLL_CASES],
    }
    for c in col_map.values():
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

    # Run 3 pipelines: price-only, quantity-only, any
    price_rows = list(alerts.aggregate(pipeline_price(start, end, METRIC, 5000),    allowDiskUse=True, maxTimeMS=180000))
    qty_rows   = list(alerts.aggregate(pipeline_quantity(start, end, METRIC, 5000), allowDiskUse=True, maxTimeMS=180000))
    any_rows   = list(alerts.aggregate(pipeline_any(start, end, METRIC, 5000),      allowDiskUse=True, maxTimeMS=180000))

    # Split each set into categories and take top-N within each
    price_by_cat = split_by_category(price_rows)
    qty_by_cat   = split_by_category(qty_rows)
    any_by_cat   = split_by_category(any_rows)

    for cat, collection in col_map.items():
        doc = {
            **common_meta,
            "category": cat,
            "top_price":    price_by_cat.get(cat, [])[:TOP_LIMIT],
            "top_quantity": qty_by_cat.get(cat, [])[:TOP_LIMIT],
            "top_any":      any_by_cat.get(cat, [])[:TOP_LIMIT],
        }
        collection.replace_one({"snapshot_date": start}, doc, upsert=True)
        print(f"✅ {cat.capitalize():8s} → price:{len(doc['top_price'])} qty:{len(doc['top_quantity'])} any:{len(doc['top_any'])}")

    client.close()
    print("🏁 Done.")


if __name__ == "__main__":
    main()

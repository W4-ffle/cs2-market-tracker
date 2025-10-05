# detect_spikes.py
# pip install "pymongo[srv]" python-dotenv

import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def getenv_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v.strip() if isinstance(v, str) and v.strip() else default

MONGO_URI = getenv_str("MONGO_URI", "")
DB_NAME   = getenv_str("MONGO_DB", "cs2_market")
HIST_COL  = getenv_str("HISTORY_COLLECTION", "csfloat_hourly_price_history")
ALERTS_COL= getenv_str("ALERTS_COLLECTION", "alerts")

PRICE_PCT = float(getenv_str("PRICE_PCT", "0.15"))       # 15%
QTY_PCT   = float(getenv_str("QTY_PCT", "0.50"))         # 50%
MIN_QTY_DELTA = int(getenv_str("MIN_QTY_DELTA", "10"))   # min absolute qty change

def main():
    if not MONGO_URI:
        raise SystemExit("Set MONGO_URI")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000, maxPoolSize=50, retryWrites=False)
    try:
        client.admin.command("ping")
    except Exception as e:
        print("❌ Mongo ping failed (auth/network):", e)
        return

    db = client[DB_NAME]
    hist = db[HIST_COL]

    # One pipeline: convert maps -> arrays, take last two hours, compute diffs, threshold filter, merge to alerts.
    pipeline = [
        {
            "$project": {
                "csfloat_id": 1,
                "pricesArr": { "$objectToArray": { "$ifNull": ["$hourly_prices", {}] } },
                "qtyArr":    { "$objectToArray": { "$ifNull": ["$hourly_quantity", {}] } },
            }
        },
        {
            "$project": {
                "csfloat_id": 1,
                "last2P": { "$slice": [ { "$sortArray": { "input": "$pricesArr", "sortBy": { "k": 1 } } }, -2 ] },
                "last2Q": { "$slice": [ { "$sortArray": { "input": "$qtyArr",    "sortBy": { "k": 1 } } }, -2 ] },
            }
        },
        {
            "$project": {
                "csfloat_id": 1,
                "hour": { "$cond": [ { "$gt": [ { "$size": "$last2P" }, 0 ] }, { "$arrayElemAt": ["$last2P.k", -1] }, None ] },
                "price_prev": { "$cond": [ { "$gt": [ { "$size": "$last2P" }, 1 ] }, { "$toDouble": { "$arrayElemAt": ["$last2P.v", -2] } }, None ] },
                "price_cur":  { "$cond": [ { "$gt": [ { "$size": "$last2P" }, 0 ] },  { "$toDouble": { "$arrayElemAt": ["$last2P.v", -1] } }, None ] },
                "qty_prev":   { "$cond": [ { "$gt": [ { "$size": "$last2Q" }, 1 ] },  { "$toInt":    { "$arrayElemAt": ["$last2Q.v", -2] } }, None ] },
                "qty_cur":    { "$cond": [ { "$gt": [ { "$size": "$last2Q" }, 0 ] },  { "$toInt":    { "$arrayElemAt": ["$last2Q.v", -1] } }, None ] },
            }
        },
        {
            "$addFields": {
                "price_delta": {
                    "$cond": [
                        { "$and": [ { "$ne": ["$price_prev", None] }, { "$ne": ["$price_cur", None] }, { "$ne": ["$price_prev", 0] } ] },
                        { "$divide": [ { "$subtract": ["$price_cur", "$price_prev"] }, "$price_prev" ] },
                        None
                    ]
                },
                "qty_abs": {
                    "$cond": [
                        { "$and": [ { "$ne": ["$qty_prev", None] }, { "$ne": ["$qty_cur", None] } ] },
                        { "$abs": { "$subtract": ["$qty_cur", "$qty_prev"] } },
                        None
                    ]
                },
                "qty_delta": {
                    "$cond": [
                        { "$and": [ { "$ne": ["$qty_prev", None] }, { "$ne": ["$qty_prev", 0] }, { "$ne": ["$qty_cur", None] } ] },
                        { "$divide": [ { "$subtract": ["$qty_cur", "$qty_prev"] }, "$qty_prev" ] },
                        None
                    ]
                }
            }
        },
        {
            "$match": {
                "$or": [
                    { "price_delta": { "$ne": None, "$gte":  PRICE_PCT } },
                    { "price_delta": { "$ne": None, "$lte": -PRICE_PCT } },
                    {
                        "$and": [
                            { "qty_abs": { "$ne": None, "$gte": MIN_QTY_DELTA } },
                            { "$or": [
                                { "qty_delta": { "$gte":  QTY_PCT } },
                                { "qty_delta": { "$lte": -QTY_PCT } }
                            ]}
                        ]
                    }
                ]
            }
        },
        {   # Convert hour key "YYYY-MM-DDTHH:00" -> real Date (UTC)
            "$addFields": { "hour_dt": { "$toDate": { "$concat": ["$hour", ":00Z"] } } }
        },
        {
            "$project": {
                "_id": 0,
                "csfloat_id": 1,
                "hour": "$hour_dt",
                "changes": [
                    { "$cond": [
                        { "$ne": ["$price_delta", None] },
                        { "field": "price",
                          "prev": "$price_prev", "cur": "$price_cur",
                          "abs_change": { "$subtract": ["$price_cur", "$price_prev"] },
                          "pct_change": "$price_delta" },
                        "$$REMOVE"
                    ]},
                    { "$cond": [
                        { "$and": [ { "$ne": ["$qty_abs", None] }, { "$ne": ["$qty_delta", None] } ] },
                        { "field": "quantity",
                          "prev": "$qty_prev", "cur": "$qty_cur",
                          "abs_change": { "$subtract": ["$qty_cur", "$qty_prev"] },
                          "pct_change": "$qty_delta" },
                        "$$REMOVE"
                    ]}
                ]
            }
        },
        {
            "$merge": {
                "into": ALERTS_COL,
                "on": ["csfloat_id", "hour"],
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    db.command("ping")  # one more sanity ping
    # Run pipeline with a conservative 2-minute max
    list(hist.aggregate(pipeline, allowDiskUse=True, maxTimeMS=120000))
    now = datetime.now(timezone.utc).isoformat()
    print(f"✅ Spike detection pipeline completed at {now}")

if __name__ == "__main__":
    main()

# csfloat_current_prices_hourly.py
# pip install "pymongo[srv]" requests python-dotenv

import os
from datetime import datetime, timezone
from math import ceil
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo import MongoClient, UpdateOne, WriteConcern
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
MONGO_URI = os.getenv("MONGO_URI")
CSFLOAT_URL = os.getenv("CSFLOAT_URL")
TIMEOUT = int(os.getenv("CSFLOAT_TIMEOUT"))
BULK_CHUNK = int(os.getenv("BULK_CHUNK"))
BULK_WORKERS = int(os.getenv("BULK_WORKERS"))
SNAPSHOT_W0 = os.getenv("SNAPSHOT_W0") == "1"
POOL_SIZE = int(os.getenv("MONGO_POOL"))

if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

# ---------- MONGO (pooling; compressors only if present) ----------
compressors = []
try:
    import zstandard  # type: ignore # noqa: F401
    compressors.append("zstd")
except Exception:
    pass
try:
    import snappy  # type: ignore # noqa: F401
    compressors.append("snappy")
except Exception:
    pass
compressors.append("zlib")

client_kwargs = dict(
    serverSelectionTimeoutMS=5000,
    maxPoolSize=POOL_SIZE,
    retryWrites=False,  # idempotent upserts; skip extra handshake
)
if compressors:
    client_kwargs["compressors"] = ",".join(compressors)

client = MongoClient(MONGO_URI, **client_kwargs)
db = client["cs2_market"]

# Write concerns: snapshot can be w=0 (optional), history stays w=1
wc_snapshot = WriteConcern(w=0, j=False) if SNAPSHOT_W0 else WriteConcern(w=1, j=False)
wc_history = WriteConcern(w=1, j=False)

current_col = db.get_collection("current_prices", write_concern=wc_snapshot)
hourly_hist_col = db.get_collection("csfloat_hourly_price_history", write_concern=wc_history)

# ---------- HTTP (keep-alive + retries + gzip) ----------
_session = None
def session():
    global _session
    if _session is None:
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=64,
            pool_maxsize=64,
            max_retries=Retry(
                total=3,
                backoff_factor=0.25,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
            ),
        )
        s.mount("https://", adapter)
        s.headers.update({
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "csfloat-hourly-snapshot/1.2",
        })
        _session = s
    return _session

# ---------- HELPERS ----------
def fetch_csfloat_price_list():
    r = session().get(CSFLOAT_URL, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        data = data.get("results") or data.get("data") or []
    return data

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def bulk_write(col, ops, name):
    if not ops:
        return (name, 0, None)
    try:
        # ordered=False gives best throughput and resilience to dup key races
        res = col.bulk_write(ops, ordered=False)
        return (name, getattr(res, "upserted_count", 0) + getattr(res, "modified_count", 0), None)
    except Exception as e:
        return (name, 0, e)

# ---------- MAIN ----------
def run_hourly_price_snapshot():
    now = datetime.now(timezone.utc)
    hour_key = now.strftime("%Y-%m-%dT%H:00")  # e.g., "2025-10-05T14:00"

    print(f"[{now.isoformat()}] Fetching CSFloat prices and updating hourly history...")

    try:
        items = fetch_csfloat_price_list()
    except Exception as e:
        print("❌ Failed to fetch CSFloat price list:", e)
        return

    # Build ops in memory, then write in parallel chunks
    ops_current, ops_history = [], []
    total = 0

    UpdateOne_ = UpdateOne
    hp_key = f"hourly_prices.{hour_key}"
    hq_key = f"hourly_quantity.{hour_key}"

    for it in items:
        csfloat_id = it.get("market_hash_name")
        if not csfloat_id:
            continue

        price = it.get("min_price")
        qty = it.get("qty")

        # minimal snapshot doc
        ops_current.append(
            UpdateOne_(
                {"csfloat_id": csfloat_id},
                {"$set": {
                    "csfloat_id": csfloat_id,
                    "price": price,
                    "quantity": qty,
                    "source": "csfloat",
                    "fetched_at": now,  # server-side $currentDate could be used too
                }},
                upsert=True,
            )
        )

        # history: set hour price/qty
        ops_history.append(
            UpdateOne_(
                {"csfloat_id": csfloat_id},
                {"$set": {
                    hp_key: price,
                    hq_key: qty,
                    "last_fetched_at": now,
                }},
                upsert=True,
            )
        )

        total += 1

    # Parallel flush in chunks
    futures = []
    with ThreadPoolExecutor(max_workers=BULK_WORKERS) as ex:
        for cur_chunk, hist_chunk in zip(
            chunk(ops_current, BULK_CHUNK),
            chunk(ops_history, BULK_CHUNK)
        ):
            futures.append(ex.submit(bulk_write, current_col, cur_chunk, "current"))
            futures.append(ex.submit(bulk_write, hourly_hist_col, hist_chunk, "history"))

        # If counts differ, flush the remainder (zip stops at shortest)
        rem_cur = list(chunk(ops_current, BULK_CHUNK))
        rem_hist = list(chunk(ops_history, BULK_CHUNK))
        if len(rem_cur) != len(rem_hist):
            longer, col, label = (
                (rem_cur, current_col, "current")
                if len(rem_cur) > len(rem_hist) else
                (rem_hist, hourly_hist_col, "history")
            )
            # Only submit the remaining chunks beyond the zipped ones
            extra = longer[min(len(rem_cur), len(rem_hist)):]
            for extra_chunk in extra:
                futures.append(ex.submit(bulk_write, col, extra_chunk, label))

        errs = []
        done = 0
        for f in as_completed(futures):
            name, count, err = f.result()
            done += count
            if err:
                errs.append((name, err))

    if errs:
        for name, e in errs:
            print(f"⚠️ {name} bulk_write error: {e}")

    print(f"✅ Hourly snapshot complete. Updated {total} items.")

# ---------- ENTRY ----------
if __name__ == "__main__":
    run_hourly_price_snapshot()

# cleanup_old_alerts.py
# Deletes alerts older than 24 hours from MongoDB
# pip install "pymongo[srv]" python-dotenv

import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
ALERTS_COLLECTION = os.getenv("ALERTS_COLLECTION")

if not MONGO_URI:
    raise SystemExit("❌ Set MONGO_URI in .env")

# ---------- MAIN ----------
def cleanup_old_alerts():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    col = db[ALERTS_COLLECTION]

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    print(f"[{now.isoformat()}] Cleaning alerts older than {cutoff.isoformat()}...")

    try:
        result = col.delete_many({"hour": {"$lt": cutoff}})
        print(f"✅ Deleted {result.deleted_count} old alerts.")
    except Exception as e:
        print("❌ Error deleting old alerts:", e)
    finally:
        client.close()

if __name__ == "__main__":
    cleanup_old_alerts()

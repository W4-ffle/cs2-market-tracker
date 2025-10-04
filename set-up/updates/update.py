# update_all_item_names_with_csfloat_id.py
# pip install "pymongo[srv]" python-dotenv

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ----------------- CONFIG -----------------
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise SystemExit("Set MONGO_URI in .env")

# ----------------- MongoDB -----------------
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["cs2_market"]
csfloat_daily_col = db["csfloat_daily_prices"]
all_item_names_col = db["all_item_names"]

def link_all_item_names_to_csfloat():
    total_updated = 0
    for item_doc in all_item_names_col.find():
        raw_name = item_doc.get("raw_name")
        csfloat_doc = csfloat_daily_col.find_one({"item_name": raw_name})
        if csfloat_doc:
            all_item_names_col.update_one(
                {"_id": item_doc["_id"]},
                {"$set": {"csfloat_id": csfloat_doc["item_name"]}}
            )
            total_updated += 1
    print(f"Updated {total_updated} all_item_names documents with csfloat_id.")

if __name__ == "__main__":
    link_all_item_names_to_csfloat()

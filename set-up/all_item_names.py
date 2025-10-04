# populate_all_item_names.py
# pip install "pymongo[srv]" python-dotenv

import os, re
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

# ----------------- Helpers -----------------
def extract_keywords(item_name):
    """
    Extract lowercase keywords for search.
    Keeps letters, numbers, |, *, /, -, _, [], ()
    """
    cleaned = re.sub(r"[^\w\*\|\-/\(\)\[\]\s]", " ", item_name)
    tokens = re.split(r"[ \|\-/\(\)\[\]_]+", cleaned.lower())
    return [t for t in tokens if t]

# ----------------- Main -----------------
def populate_all_item_names():
    print("Populating all_item_names from csfloat_daily_prices...")

    # drop the collection since this is a rebuild
    all_item_names_col.drop()
    print("Dropped existing all_item_names collection.")

    # find unique item names in csfloat_daily_prices
    unique_items = csfloat_daily_col.distinct("item_name")

    total_inserted = 0
    for name in unique_items:
        keywords = extract_keywords(name)
        doc = {
            "raw_name": name,
            "keywords": keywords
        }
        all_item_names_col.insert_one(doc)
        total_inserted += 1

    print(f"Inserted {total_inserted} unique items into all_item_names.")

# ----------------- Run -----------------
if __name__ == "__main__":
    populate_all_item_names()

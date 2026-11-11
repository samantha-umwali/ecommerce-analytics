import json
from pathlib import Path
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parent.parent   # C:\ecommerce-analytics
db = MongoClient("mongodb://localhost:27017")["ecommerce"]

def find(name):
    for p in (ROOT / "data" / f"{name}.json", ROOT / f"{name}.json"):
        if p.exists():
            return p
    return None

for name in ["users", "products", "categories", "transactions"]:
    path = find(name)
    if path is None:
        print(f"{name}: FILE NOT FOUND (run the generator first)")
        continue
    with open(path, "r", encoding="utf-8") as f:
        docs = json.load(f)
    db[name].drop()
    for i in range(0, len(docs), 5000):
        db[name].insert_many(docs[i:i + 5000])
    print(f"{name}: read {len(docs):,} from {path.name} | in MongoDB: {db[name].count_documents({}):,}")
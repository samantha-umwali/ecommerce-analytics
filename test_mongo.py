
from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017/")

print("SERVER:")
print(client.admin.command("hello"))

print("\nDATABASE:")
print(client["ecommerce"].command("dbStats"))

print("\nCOUNTS:")
db = client["ecommerce"]

for collection in ["users", "products", "transactions"]:
    print(collection, db[collection].count_documents({}))

from pymongo import MongoClient

db = MongoClient("mongodb://localhost:27017")["ecommerce"]

# Pipeline 1: Top 10 products by revenue
print("=== Top 10 products by revenue ===")
pipeline1 = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
    }},
    {"$sort": {"revenue": -1}},
    {"$limit": 10},
]
for r in db.transactions.aggregate(pipeline1):
    print(f"{r['_id']}  revenue=${r['revenue']:,.2f}  units={r['units_sold']}")

# Pipeline 2: Revenue by category (joins transactions -> products)
print("\n=== Revenue by category (top 10) ===")
pipeline2 = [
    {"$unwind": "$items"},
    {"$lookup": {
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "product_id",
        "as": "product",
    }},
    {"$unwind": "$product"},
    {"$group": {
        "_id": "$product.category_id",
        "revenue": {"$sum": "$items.subtotal"},
        "orders": {"$sum": 1},
    }},
    {"$sort": {"revenue": -1}},
    {"$limit": 10},
]
for r in db.transactions.aggregate(pipeline2):
    print(f"{r['_id']}  revenue=${r['revenue']:,.2f}  line_items={r['orders']}")
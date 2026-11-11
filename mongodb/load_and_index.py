from pymongo import MongoClient

# Connect to the database
db = MongoClient("mongodb://localhost:27017")["ecommerce"]

# Create indexes as per instructions
db.transactions.create_index("user_id")
db.transactions.create_index("items.product_id")
db.products.create_index("category_id")

# Print document counts to verify
print("counts:", db.users.count_documents({}), db.products.count_documents({}), db.transactions.count_documents({}))
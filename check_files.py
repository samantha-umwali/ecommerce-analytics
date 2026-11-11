
import json
import time
import os

files = [
    "data/users.json",
    "data/products.json",
    "data/categories.json",
    "data/transactions.json"
]

for f in files:
    start = time.time()

    if os.path.exists(f):
        print("Reading:", f, flush=True)

        with open(f, "r", encoding="utf-8") as file:
            data = json.load(file)

        print(f, len(data), "records")
        print("Time:", round(time.time() - start, 2), "seconds")

    else:
        print(f, "NOT FOUND")
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
VIZ = ROOT / "visualizations"
VIZ.mkdir(exist_ok=True)

# --- 1) MongoDB: which sessions ended in a purchase? ---
db = MongoClient("mongodb://localhost:27017")["ecommerce"]
purchased_ids = [s for s in db.transactions.distinct("session_id") if s]
print(f"MongoDB: {len(purchased_ids):,} transactions linked to a session")

ids_csv = ROOT / "data" / "purchased_sessions.csv"
pd.DataFrame({"session_id": purchased_ids}).to_csv(ids_csv, index=False)

# --- 2) Spark: join with cleaned sessions ---
spark = (SparkSession.builder.master("local[*]").appName("Funnel")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

sessions = spark.read.parquet((PROCESSED / "sessions").as_posix())
purchased = (spark.read.option("header", True).csv(ids_csv.as_posix())
             .withColumn("purchased", F.lit(True)))

flags = (sessions.join(purchased, "session_id", "left")
         .withColumn("purchased", F.coalesce(F.col("purchased"), F.lit(False)))
         # each stage requires the previous one, so the funnel only goes down
         .withColumn("s_view", F.col("viewed_product"))
         .withColumn("s_cart", F.col("s_view") & F.col("reached_cart"))
         .withColumn("s_checkout", F.col("s_cart") & F.col("reached_checkout"))
         .withColumn("s_purchase", F.col("s_checkout") & F.col("purchased")))

r = flags.agg(
    F.count("*").alias("all_sessions"),
    F.sum(F.col("s_view").cast("int")).alias("view"),
    F.sum(F.col("s_cart").cast("int")).alias("cart"),
    F.sum(F.col("s_checkout").cast("int")).alias("checkout"),
    F.sum(F.col("s_purchase").cast("int")).alias("purchase"),
).first()

# --- 3) Save with pandas (no createDataFrame needed) ---
stages = ["All Sessions", "Product View", "Cart", "Checkout", "Purchase"]
counts = [r["all_sessions"], r["view"], r["cart"], r["checkout"], r["purchase"]]
funnel = pd.DataFrame({"Stage": stages, "Count": counts})
funnel["Pct_of_Sessions"] = (100 * funnel["Count"] / funnel["Count"].iloc[0]).round(2)
funnel["Pct_of_Previous"] = (100 * funnel["Count"] / funnel["Count"].shift(1)).round(2)

print(funnel.to_string(index=False))
funnel.to_csv(VIZ / "funnel.csv", index=False)
print("Saved visualizations/funnel.csv")

spark.stop()
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = DATA / "processed"
fp = lambda p: Path(p).as_posix()

spark = (SparkSession.builder.master("local[*]").appName("CleanData")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

def read_json(path, schema=None):
    r = spark.read.option("multiLine", True)
    if schema:
        r = r.schema(schema)
    return r.json(fp(path))

# ---- users / products / categories ----
users = (read_json(DATA / "users.json").dropna(subset=["user_id"]).dropDuplicates(["user_id"])
         .select("user_id",
                 F.col("geo_data.city").alias("city"),
                 F.col("geo_data.state").alias("state"),
                 F.col("geo_data.country").alias("country"),
                 F.to_timestamp("registration_date").alias("registration_date"),
                 F.to_timestamp("last_active").alias("last_active")))

products = (read_json(DATA / "products.json").dropDuplicates(["product_id"])
            .select("product_id", "name", "category_id", "base_price", "current_stock", "is_active",
                    F.to_timestamp("creation_date").alias("creation_date")))

categories = read_json(DATA / "categories.json").select(
    "category_id", F.col("name").alias("category_name"))

# ---- transactions + exploded line items ----
raw_txn = (read_json(DATA / "transactions.json")
           .dropna(subset=["user_id"]).dropDuplicates(["transaction_id"])
           .withColumn("txn_time", F.to_timestamp("timestamp")))

txn = raw_txn.select("transaction_id", "session_id", "user_id", "txn_time",
                     "subtotal", "discount", "total", "payment_method", "status")

txn_items = (raw_txn.select("transaction_id", "user_id", "txn_time", F.explode("items").alias("it"))
             .select("transaction_id", "user_id", "txn_time",
                     F.col("it.product_id").alias("product_id"),
                     F.col("it.quantity").alias("quantity"),
                     F.col("it.unit_price").alias("unit_price"),
                     F.col("it.subtotal").alias("line_total")))

# ---- sessions: EXPLICIT schema (no cart_contents -> no 5,000-column blowup) ----
page_view = StructType([
    StructField("timestamp", StringType()), StructField("page_type", StringType()),
    StructField("product_id", StringType()), StructField("category_id", StringType()),
    StructField("view_duration", LongType())])
session_schema = StructType([
    StructField("session_id", StringType()), StructField("user_id", StringType()),
    StructField("start_time", StringType()), StructField("end_time", StringType()),
    StructField("duration_seconds", LongType()),
    StructField("geo_data", StructType([
        StructField("city", StringType()), StructField("state", StringType()),
        StructField("country", StringType()), StructField("ip_address", StringType())])),
    StructField("device_profile", StructType([
        StructField("type", StringType()), StructField("os", StringType()),
        StructField("browser", StringType())])),
    StructField("viewed_products", ArrayType(StringType())),
    StructField("page_views", ArrayType(page_view)),
    StructField("conversion_status", StringType()),
    StructField("referrer", StringType())])

sessions = (read_json(DATA / "sessions_*.json", session_schema)
            .dropna(subset=["user_id"]).dropDuplicates(["session_id"])
            .select("session_id", "user_id",
                    F.to_timestamp("start_time").alias("start_time"),
                    F.to_timestamp("end_time").alias("end_time"),
                    "duration_seconds",
                    F.col("geo_data.country").alias("country"),
                    F.col("device_profile.type").alias("device_type"),
                    F.col("device_profile.os").alias("os"),
                    F.col("device_profile.browser").alias("browser"),
                    "referrer", "conversion_status",
                    F.size("viewed_products").alias("n_products_viewed"),
                    F.size("page_views").alias("n_page_views"),
                    F.exists("page_views", lambda x: x["page_type"] == "product_detail").alias("viewed_product"),
                    F.exists("page_views", lambda x: x["page_type"] == "cart").alias("reached_cart"),
                    F.exists("page_views", lambda x: x["page_type"] == "checkout").alias("reached_checkout"),
                    F.exists("page_views", lambda x: x["page_type"] == "confirmation").alias("reached_confirmation")))

# ---- write Parquet ----
for name, df in [("users", users), ("products", products), ("categories", categories),
                 ("transactions", txn), ("transaction_items", txn_items), ("sessions", sessions)]:
    df.write.mode("overwrite").parquet(fp(OUT / name))
    print(f"{name}: {df.count():,} rows -> data/processed/{name}")

print("Data cleaned and written to Parquet!")
spark.stop()
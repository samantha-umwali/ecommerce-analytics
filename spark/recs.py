import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent.parent          # C:\ecommerce-analytics
PROCESSED = ROOT / "data" / "processed"
VIZ = ROOT / "visualizations"
VIZ.mkdir(exist_ok=True)

def fp(p):
    return Path(p).as_posix()

def save_csv(df, path):
    # replaces df.toPandas(), which fails with "No module named 'distutils'"
    pd.DataFrame([r.asDict() for r in df.collect()], columns=df.columns).to_csv(path, index=False)
    print("saved", path)

spark = (SparkSession.builder.master("local[*]").appName("Recs")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

ti = spark.read.parquet(fp(PROCESSED / "transaction_items"))
products = spark.read.parquet(fp(PROCESSED / "products"))
cats = spark.read.parquet(fp(PROCESSED / "categories"))

# 1) Product pairs bought in the same transaction
t = ti.select("transaction_id", "product_id").distinct()
a, b = t.alias("a"), t.alias("b")
prod_pairs = (
    a.join(b, (F.col("a.transaction_id") == F.col("b.transaction_id"))
              & (F.col("a.product_id") < F.col("b.product_id")))
    .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
    .agg(F.count("*").alias("times_together"))
    .orderBy(F.desc("times_together"), "product_a", "product_b")
    .limit(50)
)
print("=== Product pairs (bought together) ===")
prod_pairs.show(10, truncate=False)
save_csv(prod_pairs, VIZ / "recs.csv")

# 2) Category pairs: customers who bought from category A also bought from category B
uc = (ti.join(products.select("product_id", "category_id"), "product_id")
        .select("user_id", "category_id").distinct())
ua, ub = uc.alias("ua"), uc.alias("ub")
cat_pairs = (
    ua.join(ub, (F.col("ua.user_id") == F.col("ub.user_id"))
                & (F.col("ua.category_id") < F.col("ub.category_id")))
    .groupBy(F.col("ua.category_id").alias("cat_a"), F.col("ub.category_id").alias("cat_b"))
    .agg(F.count("*").alias("shared_buyers"))
    .join(cats.withColumnRenamed("category_id", "cat_a").withColumnRenamed("category_name", "name_a"), "cat_a")
    .join(cats.withColumnRenamed("category_id", "cat_b").withColumnRenamed("category_name", "name_b"), "cat_b")
    .select("cat_a", "name_a", "cat_b", "name_b", "shared_buyers")
    .orderBy(F.desc("shared_buyers"), "cat_a", "cat_b")
    .limit(20)
)
print("=== Category pairs (same customers) ===")
cat_pairs.show(10, truncate=False)
save_csv(cat_pairs, VIZ / "recs_categories.csv")

spark.stop()
print("Done.")
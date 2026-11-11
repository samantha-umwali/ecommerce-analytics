import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession

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

spark = (SparkSession.builder.master("local[*]").appName("SQLAnalytics")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Load cleaned data (all written by clean_data.py)
for folder, view in [("transactions", "txn"), ("transaction_items", "txn_items"),
                     ("products", "prod"), ("categories", "cat"),
                     ("users", "users"), ("sessions", "sessions")]:
    spark.read.parquet(fp(PROCESSED / folder)).createOrReplaceTempView(view)

queries = {
    # 1) your original query, fixed: every line item counts, not just items[0]
    "revenue_by_category": """
        SELECT p.category_id, c.category_name,
               ROUND(SUM(ti.line_total), 2) AS revenue,
               SUM(ti.quantity)             AS units
        FROM txn_items ti
        JOIN prod p ON ti.product_id = p.product_id
        JOIN cat  c ON p.category_id = c.category_id
        GROUP BY p.category_id, c.category_name
        ORDER BY revenue DESC
    """,
    # 2) revenue per day
    "daily_revenue": """
        SELECT CAST(DATE(txn_time) AS STRING) AS day,
               COUNT(*) AS orders,
               ROUND(SUM(total), 2) AS revenue
        FROM txn
        GROUP BY DATE(txn_time)
        ORDER BY day
    """,
    # 3) conversion rate by device
    "conversion_by_device": """
        SELECT device_type,
               COUNT(*) AS sessions,
               SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted,
               ROUND(100.0 * SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                     / COUNT(*), 2) AS conversion_pct
        FROM sessions
        GROUP BY device_type
        ORDER BY conversion_pct DESC
    """,
    # 4) top countries by revenue
    "revenue_by_country": """
        SELECT u.country, COUNT(*) AS orders, ROUND(SUM(t.total), 2) AS revenue
        FROM txn t JOIN users u ON t.user_id = u.user_id
        GROUP BY u.country
        ORDER BY revenue DESC
        LIMIT 10
    """,
}

for name, sql in queries.items():
    print(f"\n--- {name} ---")
    result = spark.sql(sql)
    result.show(10, truncate=False)
    save_csv(result, VIZ / f"{name}.csv")

spark.stop()
print("Done.")
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

VIZ = Path(__file__).resolve().parent
sns.set_theme(style="whitegrid")

def load(name):
    p = VIZ / name
    if not p.exists():
        print(f"SKIPPED: {name} not found (run the Spark scripts first)")
        return None
    return pd.read_csv(p)

def save(fig_name):
    plt.tight_layout()
    plt.savefig(VIZ / fig_name, dpi=150)
    plt.close()
    print(f"Saved {fig_name}")

# 1) Conversion funnel
df = load("funnel.csv")
if df is not None:
    plt.figure(figsize=(9, 5))
    ax = sns.barplot(data=df, x="Stage", y="Count", hue="Stage", palette="viridis", legend=False)
    for i, row in df.iterrows():
        ax.text(i, row["Count"], f"{int(row['Count']):,}\n({row['Pct_of_Sessions']}%)",
                ha="center", va="bottom", fontsize=9)
    ax.set_ylim(0, df["Count"].max() * 1.18)
    plt.title("E-commerce Conversion Funnel")
    save("funnel_chart.png")

# 2) Revenue by category (top 10)
df = load("revenue_by_category.csv")
if df is not None:
    d = df.head(10).copy()
    d["label"] = d["category_id"] + " " + d["category_name"].str.slice(0, 22)
    plt.figure(figsize=(10, 6))
    sns.barplot(data=d, y="label", x="revenue", hue="label", palette="mako", legend=False)
    plt.title("Top 10 Categories by Revenue")
    plt.xlabel("Revenue ($)"); plt.ylabel("")
    save("revenue_by_category.png")

# 3) Daily revenue trend
df = load("daily_revenue.csv")
if df is not None:
    df["day"] = pd.to_datetime(df["day"])
    plt.figure(figsize=(11, 5))
    plt.plot(df["day"], df["revenue"], linewidth=1.8)
    plt.fill_between(df["day"], df["revenue"], alpha=0.2)
    plt.title("Daily Revenue")
    plt.xlabel("Date"); plt.ylabel("Revenue ($)")
    plt.xticks(rotation=30)
    save("daily_revenue.png")

# 4) Conversion rate by device
df = load("conversion_by_device.csv")
if df is not None:
    plt.figure(figsize=(7, 5))
    ax = sns.barplot(data=df, x="device_type", y="conversion_pct", hue="device_type",
                     palette="rocket", legend=False)
    for i, v in enumerate(df["conversion_pct"]):
        ax.text(i, v, f"{v}%", ha="center", va="bottom")
    plt.title("Session Conversion Rate by Device")
    plt.xlabel("Device"); plt.ylabel("Conversion (%)")
    save("conversion_by_device.png")

# 5) Recommendations: top category pairs bought by the same users
df = load("recs_categories.csv")
if df is not None:
    d = df.head(10).copy()
    d["pair"] = d["cat_a"] + " + " + d["cat_b"]
    plt.figure(figsize=(10, 6))
    sns.barplot(data=d, y="pair", x="shared_buyers", hue="pair", palette="crest", legend=False)
    plt.title("Top Category Pairs Bought by the Same Customers")
    plt.xlabel("Shared buyers"); plt.ylabel("")
    save("recs_chart.png")
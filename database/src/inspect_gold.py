import pandas as pd
from pathlib import Path

GOLD_DIR = Path("data/gold")

primary_keys = {
    "order_facts": ["order_id"],
    "order_line_revenue": ["order_id", "product_id"],
    "daily_revenue": ["payment_date"],
    "daily_orders": ["event_date"],
    "products": ["product_id"],
    "product_daily": ["payment_date", "product_id"],
    "revenue_by_category": ["category"],
    "session_facts": ["session_id"],
    "customer_behaviour": ["customer_id"],
    "funnel_daily": ["event_date"],
    "revenue_allocation_check": ["order_id"]
}

for dataset, keys in primary_keys.items():

    folder = GOLD_DIR / dataset
    parquet_file = next(folder.glob("*.parquet"))

    df = pd.read_parquet(parquet_file)

    duplicate_count = df.duplicated(subset=keys).sum()

    print(f"\nDataset: {dataset}")
    print("Primary key candidate:", keys)
    print("Duplicate key combinations:", duplicate_count)

    if duplicate_count == 0:
        print("Result: UNIQUE")
    else:
        print("Result: NOT UNIQUE")
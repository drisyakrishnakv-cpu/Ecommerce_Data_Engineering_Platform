
import os
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, text

# --------------------------------------------------
# 1. Database configuration
# --------------------------------------------------

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL)

GOLD_DIR = Path("data/gold")

# --------------------------------------------------
# 2. Primary keys for all Gold tables
# --------------------------------------------------

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

# --------------------------------------------------
# 3. Read and validate all Gold datasets
# --------------------------------------------------

datasets = {}

metadata = MetaData()
metadata.reflect(bind=engine, schema="public")

for table_name, keys in primary_keys.items():

    print(f"\nReading dataset: {table_name}")

    folder = GOLD_DIR / table_name

    # Read only actual Parquet files
    parquet_files = list(folder.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found in {folder}"
        )

    df = pd.concat(
        [pd.read_parquet(file) for file in parquet_files],
        ignore_index=True
    )

    table = metadata.tables[f"public.{table_name}"]

    # Validate column names
    expected_columns = set(table.columns.keys())
    actual_columns = set(df.columns)

    if expected_columns != actual_columns:
        raise ValueError(
            f"Column mismatch in {table_name}"
        )

    # Validate missing values
    if df.isnull().any().any():
        raise ValueError(
            f"Missing values found in {table_name}"
        )

    # Validate primary-key uniqueness
    if df.duplicated(subset=keys).any():
        raise ValueError(
            f"Duplicate primary keys found in {table_name}"
        )

    # Validate primary keys are not null
    if df[keys].isnull().any().any():
        raise ValueError(
            f"Null primary key found in {table_name}"
        )

    datasets[table_name] = df

    print(f"Validated {len(df)} records")


# --------------------------------------------------
# 4. Full refresh inside one transaction
# --------------------------------------------------

print("\nStarting PostgreSQL full refresh...")

try:

    with engine.begin() as connection:

        # Remove existing snapshot data
        for table_name in primary_keys:

            connection.execute(
                text(f'TRUNCATE TABLE public."{table_name}"')
            )

        print("Existing records cleared.")

        # Insert complete regenerated Gold datasets
        for table_name, df in datasets.items():

            table = metadata.tables[f"public.{table_name}"]

            records = df.to_dict(orient="records")

            if records:
                connection.execute(
                    table.insert(),
                    records
                )

            print(
                f"Loaded {len(records)} records into {table_name}"
            )

    print("\nFULL REFRESH COMPLETED SUCCESSFULLY!")

except Exception as error:

    print("\nFULL REFRESH FAILED!")
    print("Error:", error)
    print("All database changes have been rolled back.")

    raise

finally:

    engine.dispose()

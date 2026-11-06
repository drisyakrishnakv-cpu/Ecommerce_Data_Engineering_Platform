import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
import os

# Load database credentials
load_dotenv()

# PostgreSQL connection
DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL)

GOLD_DIR = Path("data/gold")

# Dataset names and their primary keys
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

# Load each dataset
for table_name, keys in primary_keys.items():

    print(f"\nLoading: {table_name}")

    folder = GOLD_DIR / table_name
    parquet_file = next(folder.glob("*.parquet"))

    df = pd.read_parquet(parquet_file)

    # Convert pandas data to records
    records = df.to_dict(orient="records")

    # Prepare UPSERT statement
    metadata = __import__("sqlalchemy").MetaData()
    from sqlalchemy import Table

    table = Table(
        table_name,
        metadata,
        autoload_with=engine
    )

    stmt = insert(table).values(records)

    update_values = {
        column.name: stmt.excluded[column.name]
        for column in table.columns
        if column.name not in keys
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=keys,
        set_=update_values
    )

    # Execute database operation
    with engine.begin() as connection:
        connection.execute(stmt)

    print(f"Successfully loaded {len(df)} rows into {table_name}")

print("\nAll Gold datasets loaded successfully!")
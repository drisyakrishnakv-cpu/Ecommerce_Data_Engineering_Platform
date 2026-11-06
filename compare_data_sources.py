
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME")
)

engine = create_engine(url, pool_pre_ping=True)

gold_dir = PROJECT_ROOT / "data" / "gold"

tables = [
    "order_facts",
    "order_line_revenue",
    "daily_revenue",
    "daily_orders",
    "products",
    "product_daily",
    "revenue_by_category",
    "session_facts",
    "customer_behaviour",
    "funnel_daily",
    "revenue_allocation_check"
]

results = []

try:
    with engine.connect() as connection:

        for table in tables:

            folder = gold_dir / table
            parquet_files = list(folder.glob("*.parquet"))

            if not parquet_files:
                print(f"MISSING GOLD FILE: {table}")
                continue

            gold_df = pd.concat(
                [pd.read_parquet(file) for file in parquet_files],
                ignore_index=True
            )

            postgres_df = pd.read_sql_table(
                table,
                connection,
                schema="public"
            )

            gold_columns = set(gold_df.columns)
            postgres_columns = set(postgres_df.columns)

            columns_match = gold_columns == postgres_columns
            rows_match = len(gold_df) == len(postgres_df)

            results.append({
                "table": table,
                "gold_rows": len(gold_df),
                "postgres_rows": len(postgres_df),
                "columns_match": columns_match,
                "row_counts_match": rows_match
            })

            print(
                f"{'PASS' if columns_match and rows_match else 'CHECK'} "
                f"| {table} "
                f"| Gold: {len(gold_df)} "
                f"| PostgreSQL: {len(postgres_df)}"
            )

    report = pd.DataFrame(results)

    print("\nCOMPARISON SUMMARY")
    print(report.to_string(index=False))

    if not report.empty:
        all_match = (
            report["columns_match"].all()
            and report["row_counts_match"].all()
            and len(report) == len(tables)
        )

        print(
            "\nALL TABLES MATCH!"
            if all_match
            else "\nSOME TABLES NEED FURTHER CHECKING."
        )

except Exception as error:
    print("\nCOMPARISON FAILED:", error)
    raise

finally:
    engine.dispose()

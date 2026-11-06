
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# =====================================================
# PROJECT CONFIGURATION
# =====================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")


TABLES = [
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
    "revenue_allocation_check",
]


# =====================================================
# DATABASE ENGINE
# =====================================================

def create_database_engine():

    required = [
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
    ]

    missing = [
        key for key in required
        if not os.getenv(key)
    ]

    if missing:
        raise ValueError(
            f"Missing database settings: {', '.join(missing)}"
        )

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


# =====================================================
# CONNECTION STATUS
# =====================================================

def test_database_connection():

    engine = create_database_engine()

    try:
        with engine.connect() as connection:

            database = connection.execute(
                text("SELECT current_database()")
            ).scalar()

            return database

    finally:
        engine.dispose()


# =====================================================
# LOAD ALL POSTGRESQL DATASETS
# =====================================================

def load_all_datasets():

    datasets = {}

    engine = create_database_engine()

    try:

        with engine.connect() as connection:

            database = connection.execute(
                text("SELECT current_database()")
            ).scalar()

            print(f"DASHBOARD DATABASE: {database}")
            print("DATA SOURCE: POSTGRESQL")

            for table_name in TABLES:

                print(f"Loading PostgreSQL table: {table_name}")

                query = text(
                    f'SELECT * FROM public."{table_name}"'
                )

                df = pd.read_sql_query(
                    query,
                    connection
                )

                datasets[table_name] = df

                print(
                    f"Loaded {len(df)} records from {table_name}"
                )

    finally:
        engine.dispose()

    return datasets


# =====================================================
# DATASET VALIDATION
# =====================================================

def validate_datasets(datasets):

    results = []

    for table_name in TABLES:

        df = datasets[table_name]

        results.append({
            "dataset": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "missing_values": int(
                df.isnull().sum().sum()
            ),
            "duplicate_rows": int(
                df.duplicated().sum()
            ),
        })

    return pd.DataFrame(results).set_index("dataset")


# =====================================================
# DATABASE TABLE COUNTS
# =====================================================

def get_database_table_counts():

    engine = create_database_engine()

    counts = {}

    try:

        with engine.connect() as connection:

            for table_name in TABLES:

                result = connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM public."{table_name}"'
                    )
                )

                counts[table_name] = result.scalar()

    finally:
        engine.dispose()

    return counts

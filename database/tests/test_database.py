import os
import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

engine = create_engine(DATABASE_URL)


def test_database_connection():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1")).scalar()
        assert result == 1


def test_all_gold_tables_exist():
    expected_tables = {
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
    }

    with engine.connect() as connection:
        result = connection.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))

        existing_tables = {row[0] for row in result}

    assert expected_tables.issubset(existing_tables)


def test_order_count():
    with engine.connect() as connection:
        count = connection.execute(
            text("SELECT COUNT(*) FROM order_facts")
        ).scalar()

    assert count == 149


def test_revenue_allocation():
    with engine.connect() as connection:
        mismatches = connection.execute(text("""
            SELECT COUNT(*)
            FROM revenue_allocation_check
            WHERE ABS(allocation_difference) > 0.01
        """)).scalar()

    assert mismatches == 0


def test_order_ids_are_unique():
    with engine.connect() as connection:
        duplicates = connection.execute(text("""
            SELECT COUNT(*)
            FROM (
                SELECT order_id
                FROM order_facts
                GROUP BY order_id
                HAVING COUNT(*) > 1
            ) AS duplicate_orders
        """)).scalar()

    assert duplicates == 0
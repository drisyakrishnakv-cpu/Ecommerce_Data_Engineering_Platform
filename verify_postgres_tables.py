
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
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

expected_tables = [
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

try:
    with engine.connect() as connection:

        for table in expected_tables:

            result = connection.execute(
                text("""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                """),
                {"table_name": table}
            )

            exists = result.scalar() == 1

            if exists:
                count = connection.execute(
                    text(f'SELECT COUNT(*) FROM public."{table}"')
                ).scalar()

                print(f"OK  | {table:30} | {count} rows")

            else:
                print(f"MISSING | {table}")

        print("\nDATABASE TABLE VERIFICATION COMPLETED!")

except Exception as error:
    print("Verification failed:", error)

finally:
    engine.dispose()

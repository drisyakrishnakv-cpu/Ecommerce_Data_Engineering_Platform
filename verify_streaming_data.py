from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text
import os

load_dotenv(Path(__file__).resolve().parent / ".env")

engine = create_engine(
    "postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)

try:
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM streaming_events")
        ).scalar()

        print("POSTGRESQL STREAMING DATA VERIFICATION")
        print("--------------------------------------")
        print("Total streaming events:", count)

        sample = conn.execute(
            text("""
                SELECT event_id, event_type, customer_id, event_date
                FROM streaming_events
                LIMIT 5
            """)
        )

        for row in sample:
            print(row)

finally:
    engine.dispose()
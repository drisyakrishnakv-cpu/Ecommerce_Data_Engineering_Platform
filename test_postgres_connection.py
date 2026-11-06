
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Load credentials from project root
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

required = [
    "DB_HOST",
    "DB_PORT",
    "DB_NAME",
    "DB_USER",
    "DB_PASSWORD"
]

missing = [key for key in required if not os.getenv(key)]

if missing:
    raise ValueError(
        f"Missing database settings: {', '.join(missing)}"
    )

# Safely construct the connection URL
database_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT")),
    database=os.getenv("DB_NAME")
)

engine = create_engine(database_url, pool_pre_ping=True)

try:
    with engine.connect() as connection:

        result = connection.execute(
            text("""
                SELECT
                    current_database(),
                    current_user,
                    version()
            """)
        )

        db_name, db_user, version = result.fetchone()

        print("POSTGRESQL CONNECTION SUCCESSFUL!")
        print("Database:", db_name)
        print("Username:", db_user)
        print("Version:", version.split(",")[0])

except Exception as error:
    print("POSTGRESQL CONNECTION FAILED!")
    print(type(error).__name__, error)

finally:
    engine.dispose()

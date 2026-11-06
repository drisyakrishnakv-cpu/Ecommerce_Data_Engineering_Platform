
import os
import json
from pathlib import Path
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from utils.database_loader import create_database_engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.streaming_events (
    event_id TEXT PRIMARY KEY,
    customer_id TEXT,
    session_id TEXT,
    event_type TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""


INSERT_SQL = text("""
    INSERT INTO public.streaming_events (
        event_id,
        customer_id,
        session_id,
        event_type,
        event_timestamp,
        event_date,
        payload
    )
    VALUES (
        :event_id,
        :customer_id,
        :session_id,
        :event_type,
        :event_timestamp,
        :event_date,
        CAST(:payload AS JSONB)
    )
    ON CONFLICT (event_id) DO NOTHING
""")


def clean_value(value):
    """Convert pandas and NumPy values into JSON-safe Python values."""

    if value is None or pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, (date,)):
        return value.isoformat()

    return value


def get_parquet_directory():

    folder = os.getenv("MEMBER2_PARQUET_DIR")

    if not folder:
        raise ValueError(
            "MEMBER2_PARQUET_DIR is missing from .env"
        )

    path = Path(folder).expanduser()

    if not path.is_dir():
        raise FileNotFoundError(
            f"Parquet folder not found: {path}"
        )

    return path


def create_streaming_table():

    engine = create_database_engine()

    try:
        with engine.begin() as connection:
            connection.execute(text(CREATE_TABLE_SQL))
    finally:
        engine.dispose()


def load_parquet_events():

    parquet_dir = get_parquet_directory()

    parquet_files = list(parquet_dir.rglob("*.parquet"))

    if not parquet_files:
        return {
            "files_found": 0,
            "rows_read": 0,
            "inserted": 0,
            "duplicates": 0,
        }

    df = pd.read_parquet(
        parquet_dir,
        engine="pyarrow"
    )

    required = [
        "event_id",
        "customer_id",
        "session_id",
        "event_type",
        "event_timestamp",
        "event_date",
    ]

    missing = [
        column for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    df["event_timestamp"] = pd.to_datetime(
        df["event_timestamp"],
        utc=True,
        errors="coerce"
    )

    df["event_date"] = pd.to_datetime(
        df["event_date"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "event_id",
            "event_type",
            "event_timestamp",
            "event_date",
        ]
    )

    df = df.drop_duplicates(
        subset=["event_id"],
        keep="first"
    )

    records = []

    for row in df.to_dict(orient="records"):

        payload = {
            key: clean_value(value)
            for key, value in row.items()
        }

        records.append({
            "event_id": str(row["event_id"]),
            "customer_id": clean_value(row["customer_id"]),
            "session_id": clean_value(row["session_id"]),
            "event_type": str(row["event_type"]),
            "event_timestamp": row[
                "event_timestamp"
            ].to_pydatetime(),
            "event_date": row["event_date"].date(),
            "payload": json.dumps(
                payload,
                allow_nan=False
            ),
        })

    create_streaming_table()

    inserted = 0
    engine = create_database_engine()

    try:
        with engine.begin() as connection:

            batch_size = 500

            for start in range(0, len(records), batch_size):

                batch = records[
                    start:start + batch_size
                ]

                result = connection.execute(
                    INSERT_SQL,
                    batch
                )

                inserted += max(result.rowcount, 0)

    finally:
        engine.dispose()

    return {
        "files_found": len(parquet_files),
        "rows_read": len(df),
        "inserted": inserted,
        "duplicates": len(records) - inserted,
    }

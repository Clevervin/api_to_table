from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------
CSV_PATH = Path("data/processed/open_meteo_hourly.csv")
TABLE_NAME = "fact_weather_hourly"


# ------------------------------------------------------------------
# Build database engine from .env
# ------------------------------------------------------------------
def build_engine():
    load_dotenv()  # reads .env

    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "postgres")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD")

    if not password:
        raise ValueError("PG_PASSWORD is missing. Add it to your .env file.")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, future=True)


# ------------------------------------------------------------------
# Main load logic
# ------------------------------------------------------------------
def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"Missing processed CSV: {CSV_PATH}. Run transform_open_meteo.py first."
        )

    schema = os.getenv("PG_SCHEMA", "weather")
    engine = build_engine()

    # 🔍 DEBUG / CONFIRM CONNECTION
    print("ENGINE URL:", engine.url)

    df = pd.read_csv(CSV_PATH)
    df["time"] = pd.to_datetime(df["time"])

    with engine.begin() as conn:
        # Ensure schema exists
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        # Load data (replace = safe while learning)
        df.to_sql(
            TABLE_NAME,
            conn,
            schema=schema,
            if_exists="replace",
            index=False
        )

        # Add surrogate primary key
        conn.execute(
            text(
                f'''
                ALTER TABLE {schema}."{TABLE_NAME}"
                ADD COLUMN IF NOT EXISTS id BIGSERIAL;
                '''
            )
        )
        conn.execute(
            text(
                f'''
                ALTER TABLE {schema}."{TABLE_NAME}"
                ADD PRIMARY KEY (id);
                '''
            )
        )

        # Indexes
        conn.execute(
            text(
                f'''
                CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_time
                ON {schema}."{TABLE_NAME}" ("time");
                '''
            )
        )

        conn.execute(
            text(
                f'''
                CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lat_lon
                ON {schema}."{TABLE_NAME}" (latitude, longitude);
                '''
            )
        )

        row_count = conn.execute(
            text(f'SELECT COUNT(*) FROM {schema}."{TABLE_NAME}";')
        ).scalar_one()

    print(f"Loaded {row_count} rows into {schema}.{TABLE_NAME}")


# ------------------------------------------------------------------
if __name__ == "__main__":
    main()

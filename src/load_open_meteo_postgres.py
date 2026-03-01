from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

CSV_PATH = Path("data/processed/open_meteo_hourly.csv")
TABLE_NAME = "fact_weather_hourly"


def build_engine():
    load_dotenv()

    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB", "postgres")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD")

    if not password:
        raise ValueError("PG_PASSWORD is missing. Add it to your .env file.")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, future=True)


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"Missing processed CSV: {CSV_PATH}. Run transform_open_meteo.py first."
        )

    schema = os.getenv("PG_SCHEMA", "weather")
    engine = build_engine()

    print("ENGINE URL:", engine.url)

    df = pd.read_csv(CSV_PATH)
    df["time"] = pd.to_datetime(df["time"])

    # Ensure required columns exist (safety)
    required_cols = ["time", "latitude", "longitude"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for idempotent load: {missing}")

    with engine.begin() as conn:
        # 1) Ensure schema exists
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        # 2) Ensure table exists (create on first run)
        # Using append here will create the table if it doesn't exist.
        df.head(0).to_sql(
            TABLE_NAME,
            conn,
            schema=schema,
            if_exists="append",
            index=False,
        )

        # 3) Ensure surrogate PK exists
        conn.execute(
            text(
                f'''
                ALTER TABLE "{schema}"."{TABLE_NAME}"
                ADD COLUMN IF NOT EXISTS id BIGSERIAL;
                '''
            )
        )
        # Add PK only if one doesn't already exist
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conrelid = '"{schema}"."{TABLE_NAME}"'::regclass
                          AND contype = 'p'
                    ) THEN
                        ALTER TABLE "{schema}"."{TABLE_NAME}"
                        ADD PRIMARY KEY (id);
                    END IF;
                END $$;
                """
            )
        )

        # 4) Ensure UNIQUE constraint for idempotency
        conn.execute(
            text(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'uq_{TABLE_NAME}_time_lat_lon'
                    ) THEN
                        ALTER TABLE "{schema}"."{TABLE_NAME}"
                        ADD CONSTRAINT uq_{TABLE_NAME}_time_lat_lon
                        UNIQUE ("time", latitude, longitude);
                    END IF;
                END $$;
                """
            )
        )

        # 5) Ensure indexes (optional but good)
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_time ON "{schema}"."{TABLE_NAME}" ("time");'
            )
        )
        conn.execute(
            text(
                f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lat_lon ON "{schema}"."{TABLE_NAME}" (latitude, longitude);'
            )
        )

        # 6) Idempotent insert (UPSERT do nothing)
        # We insert only the data columns (exclude id)
        data_cols = [c for c in df.columns if c != "id"]
        insert_cols_sql = ", ".join([f'"{c}"' for c in data_cols])
        values_sql = ", ".join([f":{c}" for c in data_cols])

        insert_sql = f"""
            INSERT INTO "{schema}"."{TABLE_NAME}" ({insert_cols_sql})
            VALUES ({values_sql})
            ON CONFLICT ("time", latitude, longitude) DO NOTHING;
        """

        # Execute as executemany (fast enough for this dataset)
        result = conn.execute(text(insert_sql), df[data_cols].to_dict(orient="records"))
        inserted_rows = result.rowcount if result.rowcount is not None else 0

        total_rows = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{TABLE_NAME}";')
        ).scalar_one()

    print(f"Inserted {inserted_rows} new rows. Total rows now: {total_rows} in {schema}.{TABLE_NAME}")


if __name__ == "__main__":
    main()
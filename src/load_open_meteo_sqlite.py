from __future__ import annotations

from pathlib import Path
import sqlite3
import pandas as pd

CSV_PATH = Path("data/processed/open_meteo_hourly.csv")
DB_PATH = Path("data/warehouse/open_meteo.db")
TABLE_NAME = "fact_weather_hourly"


def main() -> None:
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"Missing processed CSV: {CSV_PATH}. Run transform_open_meteo.py first."
        )

    df = pd.read_csv(CSV_PATH)
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_time ON {TABLE_NAME}(time);"
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_lat_lon ON {TABLE_NAME}(latitude, longitude);"
        )

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {TABLE_NAME};"
        ).fetchone()[0]

        min_time, max_time = conn.execute(
            f"SELECT MIN(time), MAX(time) FROM {TABLE_NAME};"
        ).fetchone()

    print(f"Loaded table: {TABLE_NAME}")
    print(f"Database: {DB_PATH}")
    print(f"Rows loaded: {row_count}")
    print(f"Time range: {min_time} --> {max_time}")


if __name__ == "__main__":
    main()
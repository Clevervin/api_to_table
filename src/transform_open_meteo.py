from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd


def find_latest_raw_file(raw_dir: str = "data/raw") -> Path:
    """
    Finds the most recent Open-Meteo raw JSON file by filename timestamp.
    """
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    files = sorted(raw_path.glob("open_meteo_forecast_*.json"))
    if not files:
        raise FileNotFoundError(f"No raw Open-Meteo files found in {raw_dir}")

    return files[-1]


def load_raw_json(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def hourly_to_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    """
    Turns payload['hourly'] into a clean tabular dataframe.
    """
    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError("Unexpected payload shape: missing hourly/time")

    df = pd.DataFrame(hourly)

    # Parse timestamps
    df["time"] = pd.to_datetime(df["time"])

    # Optional metadata columns (useful later)
    df["latitude"] = payload.get("latitude")
    df["longitude"] = payload.get("longitude")
    df["timezone"] = payload.get("timezone")

    # Optional: reorder so time is first
    cols = ["time"] + [c for c in df.columns if c != "time"]
    df = df[cols]

    return df


def save_processed_csv(df: pd.DataFrame, processed_dir: str = "data/processed") -> Path:
    out_dir = Path(processed_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "open_meteo_hourly.csv"
    df.to_csv(out_path, index=False)
    return out_path


def main() -> None:
    raw_file = find_latest_raw_file("data/raw")
    payload = load_raw_json(raw_file)

    df = hourly_to_dataframe(payload)
    out_path = save_processed_csv(df, "data/processed")

    print(f"Loaded raw file: {raw_file.name}")
    print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
    print(f"Saved processed table to: {out_path}")


if __name__ == "__main__":
    main()
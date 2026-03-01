from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests
import yaml


@dataclass(frozen=True)
class OpenMeteoConfig:
    base_url: str
    latitude: float
    longitude: float
    timezone: str
    hourly: List[str]
    forecast_days: int
    raw_dir: str


def load_config(path: str = "config/config.yaml") -> OpenMeteoConfig:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    return OpenMeteoConfig(
        base_url=str(cfg["api"]["base_url"]).rstrip("/"),
        latitude=float(cfg["location"]["latitude"]),
        longitude=float(cfg["location"]["longitude"]),
        timezone=str(cfg["location"]["timezone"]),
        hourly=list(cfg["request"]["hourly"]),
        forecast_days=int(cfg["request"].get("forecast_days", 7)),
        raw_dir=str(cfg["output"]["raw_dir"]),
    )


def fetch_forecast(cfg: OpenMeteoConfig, timeout: int = 30) -> Dict[str, Any]:
    params = {
        "latitude": cfg.latitude,
        "longitude": cfg.longitude,
        "hourly": ",".join(cfg.hourly),
        "timezone": cfg.timezone,
        "forecast_days": cfg.forecast_days,
    }

    resp = requests.get(cfg.base_url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()

    if "hourly" not in payload or "time" not in payload["hourly"]:
        raise ValueError(f"Unexpected response shape. Top-level keys: {list(payload.keys())}")

    return payload


def save_raw_json(payload: Dict[str, Any], raw_dir: str) -> Path:
    out_dir = Path(raw_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"open_meteo_forecast_{ts}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return out_path


def main() -> None:
    cfg = load_config()
    payload = fetch_forecast(cfg)
    out_path = save_raw_json(payload, cfg.raw_dir)
    print(f"Saved raw Open-Meteo response to: {out_path}")


if __name__ == "__main__":
    main()
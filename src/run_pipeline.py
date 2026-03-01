from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    ("Extract (API → raw)", "extract_open_meteo.py"),
    ("Transform (raw → processed)", "transform_open_meteo.py"),
    ("Load (processed → Postgres)", "load_open_meteo_postgres.py"),
]


def run_step(label: str, script_name: str) -> None:
    script_path = PROJECT_ROOT / "src" / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Missing script: {script_path}")

    print(f"\n=== {label} ===")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(PROJECT_ROOT),
        text=True
    )

    if result.returncode != 0:
        raise SystemExit(f"\n❌ Step failed: {script_name} (exit code {result.returncode})")

    print(f"✅ Done: {script_name}")


def main() -> None:
    print("Running Open-Meteo pipeline...")

    for label, script in SCRIPTS:
        run_step(label, script)

    print("\n🎉 Pipeline completed successfully!")


if __name__ == "__main__":
    main()
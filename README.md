# api_to_table

Beginner-friendly API → clean data → analytics-ready table pipeline

---

## 🔐 Configuration & Environment Setup

This project separates **code**, **configuration**, and **secrets** to follow real-world data engineering best practices.

---
### Data Model

The pipeline loads processed weather data into a PostgreSQL analytics-ready table.

### Target Table: weather.fact_weather_hourly
### Table Description
This table contains **hourly weather observations** retrieved from the Open-Meteo API and transformed into a clean, analysis-friendly structure suitable for BI tools and SQL analytics.

### Columns

| Column Name | Data Type | Description |
|-----------|----------|------------|
| id | BIGSERIAL (PK) | Surrogate primary key |
| time | TIMESTAMP | Hourly observation timestamp |
| latitude | FLOAT | Latitude of the location |
| longitude | FLOAT | Longitude of the location |
| temperature_2m | FLOAT | Air temperature at 2 meters (°C) |
| relative_humidity_2m | FLOAT | Relative humidity (%) |
| precipitation | FLOAT | Precipitation amount (mm) |
| wind_speed_10m | FLOAT | Wind speed at 10 meters (km/h) |

### Indexes
The following indexes are created to optimize analytics and filtering:

- Index on `time` (time-series queries)
- Composite index on `(latitude, longitude)` (location-based analysis)

### Intended Usage
This table is designed for:
- Time-series analysis
- Weather trend exploration
- BI dashboards (e.g., Power BI)
- Downstream analytics and reporting

### 1. Environment Variables (`.env`)

Database credentials and other sensitive values are stored in a local `.env` file, which is **not committed to GitHub**.

Create a `.env` file in the project root with the following variables:

```env
PG_HOST=localhost
PG_PORT=5432
PG_DB=open_meteo_dw
PG_USER=postgres
PG_PASSWORD=your_postgres_password
PG_SCHEMA=weather

The .env file is intentionally excluded via .gitignore to prevent credential leakage.

2. Application Configuration (config.yaml)

Non-sensitive configuration such as API endpoints, locations, and file paths are defined in:
config/config.yaml

This file is local-only and ignored by Git. A template version is provided for reference:
config/config.example.yaml

This allows contributors to understand the expected structure without exposing secrets.

3. Why This Separation Matters

1. Security – secrets never enter version control
2. Flexibility – configuration changes don’t require code changes
3. Portability – pipeline runs across environments (local, CI, cloud)
4. Industry-standard practice for data engineering pipelines

4. Running the Pipeline

Once configuration is complete, run the pipeline steps in order:
python src/extract_open_meteo.py
python src/transform_open_meteo.py
python src/load_open_meteo_postgres.py

This pipeline will:

1. Extract hourly weather data from the Open-Meteo API
2. Transform the raw data into a clean tabular format
3. Load the data into PostgreSQL under:

weather.fact_weather_hourly

### One-command run
```bash
python src/run_pipeline.py

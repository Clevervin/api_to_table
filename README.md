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


What I Learned

This project reinforced several core data engineering and analytics concepts through hands-on implementation and debugging.

1. Configuration vs. Secrets Management

I learned the importance of clearly separating:
    Application configuration (API endpoints, locations, file paths) stored in config.yaml
    Sensitive credentials (database connection details) stored in a local .env file

This separation prevents accidental credential leaks, improves portability across environments, and mirrors real-world production practices.

2. Database vs. Schema (A Critical Distinction)

One of the key lessons was understanding the difference between:
    a database (e.g., open_meteo_dw)
    a schema (e.g., weather)
    a table (e.g., weather.fact_weather_hourly)

Misconfiguring the database name initially caused data to load successfully but appear “missing” in pgAdmin. Debugging this reinforced the importance of verifying connection targets and not relying solely on success messages.

3. Building an End-to-End, Re-runnable Pipeline

By adding a single run_pipeline.py script, I learned how small orchestration improvements can:
    simplify execution
    reduce human error
    make a pipeline feel production-ready

Being able to run the full workflow with one command (python src/run_pipeline.py) is a meaningful step beyond ad-hoc scripting.

4. Data Quality Is Not Optional
Adding basic data quality checks in the transformation step helped me internalize that:
    successful execution does not guarantee valid data
    pipelines should fail fast when assumptions are violated
    Simple checks (row counts, null timestamps, duplicates, numeric validation) significantly increase trust in downstream analytics.

5. Choosing the Right Aggregations Matters

While building the Power BI dashboard, I learned why:
    precipitation should be summed (mm accumulates)
    wind speed should be averaged (instantaneous measurement)
    temperature is best represented as an average
    sums of rates (e.g., wind speed) are misleading

These decisions directly impact insight quality and demonstrate analytical judgment.

6. Time-Series Visualization Techniques

Visualizing precipitation by day and hour highlighted the value of:
    splitting datetime fields into Date and Hour components
    using heatmaps to represent two time dimensions simultaneously
    labeling units clearly to avoid misinterpretation

This improved both interpretability and visual clarity.

7. Debugging Is a Core Skill

Perhaps the most important takeaway was that:
    most pipeline issues are configuration-related, not code-related
    printing connection targets (e.g., engine URLs) is a powerful debugging technique
    systematic verification beats trial-and-error

This project strengthened my confidence in diagnosing real-world data issues methodically.
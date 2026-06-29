# Dagtser Data Pipeline

A comprehensive data orchestration and analytics pipeline for real-time London Underground crowding data using **Dagster**, **Snowflake**, and the **TFL API**.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Running the Pipeline](#running-the-pipeline)
- [Monitoring & Scheduling](#monitoring--scheduling)
- [Technologies](#technologies)
- [Contributing](#contributing)

---

## Overview

The **Dagtser Data Pipeline** is an automated ETL (Extract, Transform, Load) system that:

- **Extracts** real-time crowding data from the Transport for London (TFL) API
- **Transforms** raw data through multi-stage processing (cleaning, validation, feature engineering)
- **Loads** processed data into Snowflake data warehouse
- **Aggregates** daily data with statistical analysis (mean, standard deviation, confidence intervals)

This pipeline provides actionable insights into London Underground station crowding patterns, enabling data-driven decision-making for transport planning and passenger experience optimization.

---

## Features

- **Real-time Data Ingestion** – Fetches live crowding data from 10+ major London stations
- **Multi-Stage ETL** – Structured data transformation pipeline with validation at each stage
- **Automated Scheduling** – Configured cron schedules for continuous data refresh
- **Statistical Aggregation** – Calculates mean, standard deviation, and 95% confidence intervals
- **Cloud Data Warehouse** – Seamless integration with Snowflake for scalable storage
- **Comprehensive Logging** – Built-in observability with Dagster's execution context
- **Modular Design** – Separated concerns with assets, resources, and utilities

---

## Architecture

```
TFL API (Crowding Data)
    ↓
[Raw Data Ingestion] → Snowflake RAW_DATA table
    ↓
[Data Cleaning] → Remove nulls, filter invalid records
    ↓
[Data Processing] → Select relevant columns, type conversions
    ↓
[Feature Engineering] → Create date/time features
    ↓
Snowflake CLEAN_DATA table
    ↓
[Daily Aggregation] → Statistical calculations (mean, std, CI)
    ↓
Snowflake AGGREGATED_DATA table
```

**Orchestration:** Dagster manages job definitions, asset dependencies, and schedule execution.

---

## Prerequisites

- Python 3.9+
- Snowflake account with database access
- TFL API key (get from [TFL Developers](https://tfl.gov.uk/info-for/open-data-users/))
- pip or conda for package management

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/YassineSdk/Dagtser_Data_Pipeline.git
cd Dagtser_Data_Pipeline
```

### 2. Create and Activate a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Key Dependencies:**
- `dagster` – Orchestration framework
- `dagster-snowflake` – Snowflake integration
- `polars` – High-performance data processing
- `pandas` – Data manipulation
- `requests` – API client
- `snowflake-connector-python` – Direct Snowflake connection
- `scipy` – Statistical computations

---

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
API_KEY=your_tfl_api_key_here

# Snowflake Configuration
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

### Monitored Stations

The pipeline currently tracks 10 major London Underground stations. To modify:

Edit `utils/get_raw_data.py` and update the `STATIONS` dictionary:

```python
STATIONS = {
    "940GZZLUKSX": "King's Cross St. Pancras",
    "940GZZLUVIC": "Victoria",
    # Add or remove stations as needed
}
```

---

## Project Structure

```
Dagtser_Data_Pipeline/
├── crowding_pipeline/          # Main pipeline package
│   ├── assets.py              # Primary ETL assets (ingestion, cleaning, processing)
│   ├── assets_2.py            # Aggregation pipeline assets
│   ├── definitions.py         # Dagster job and schedule definitions
│   ├── resources.py           # Snowflake resource configuration
│   └── __pycache__/
├── utils/                      # Utility functions
│   ├── get_raw_data.py        # TFL API integration
│   ├── get_arrivals_data.py   # Additional data retrieval (optional)
│   └── aggregation_function.py # Aggregation utilities (placeholder)
├── pyproject.toml             # Project metadata and Dagster configuration
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

---

## Data Pipeline

### Pipeline 1: Data Ingestion & Processing (tfl_pipeline_job)

**Schedule:** Every 5 minutes (`*/5 * * * *`)

| Stage | Asset | Description | Output |
|-------|-------|-------------|--------|
| Ingestion | `raw_crowding_data` | Fetches live crowding data from TFL API | Raw DataFrame |
| Loading | `load_raw_data` | Writes raw data to Snowflake | RAW_DATA table |
| Cleaning | `cleaned_crowding_data` | Removes nulls, filters invalid records | Polars DataFrame |
| Processing | `processed_data` | Selects columns, converts data types | Processed DataFrame |
| Feature Engineering | `feature_engineering` | Creates date/time features | Enriched DataFrame |
| Storage | `loading_clean_data` | Writes clean data to Snowflake | CLEAN_DATA table |

### Pipeline 2: Daily Aggregation (tfl_agg_job)

**Schedule:** Daily at 1:00 PM (`* 1 * * *`)

| Stage | Asset | Description | Metrics |
|-------|-------|-------------|---------|
| Loading | `Loading_daily_data` | Retrieves yesterday's clean data | Raw records |
| Aggregation | `aggregating_data` | Aggregates by station and date | Mean, Std Dev, CI |
| Storage | `load_aggregated_data` | Writes aggregated data to Snowflake | AGGREGATED_DATA table |

**Statistical Metrics Calculated:**
- `avg_crowding` – Mean crowding percentage
- `std_crowding` – Standard deviation
- `ci_lower` – 95% confidence interval (lower bound)
- `ci_upper` – 95% confidence interval (upper bound)

---

## Running the Pipeline

### Option 1: Using Dagster UI (Recommended)

```bash
dagster dev
```

This starts the Dagster development server at `http://localhost:3000`

From the UI, you can:
- View and trigger jobs manually
- Monitor asset dependencies
- Check execution logs in real-time
- Manage schedules

### Option 2: Command Line Execution

```bash
# Run the main pipeline job
dagster job execute -f crowding_pipeline/definitions.py -j tfl_pipeline_job

# Run the aggregation job
dagster job execute -f crowding_pipeline/definitions.py -j tfl_agg_job
```

### Option 3: Trigger Specific Assets

```bash
dagster asset materialize -f crowding_pipeline/definitions.py --select raw_crowding_data
```

---

## Monitoring & Scheduling

### Active Schedules

| Schedule | Job | Frequency | Timezone |
|----------|-----|-----------|----------|
| `tfl_schedule` | `tfl_pipeline_job` | Every 5 minutes | UTC |
| `tfl_schudule` | `tfl_agg_job` | Daily at 1:00 PM | UTC |

### Key Metrics to Monitor

- **Data Freshness** – Time since last successful ingestion
- **API Success Rate** – Percentage of successful TFL API calls
- **Data Quality** – Count of records with valid crowding data
- **Pipeline Duration** – Execution time for each stage
- **Snowflake Load Time** – Insert/append performance

### Logging

All asset executions log detailed information via Dagster's `AssetExecutionContext`:

```python
context.log.info(f"Loaded {len(df)} rows")
context.log.info(f"Data shape: {df.shape}")
```

Access logs from the Dagster UI or console output.

---

## Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Orchestration | Dagster | Workflow orchestration & asset management |
| Data Warehouse | Snowflake | Scalable cloud data storage |
| Data Processing | Polars + Pandas | Fast data transformation |
| API Integration | Requests | TFL API client |
| Statistics | SciPy | Statistical calculations |
| Configuration | Python-dotenv | Environment variable management |

---

## Database Schema

### RAW_DATA Table
```sql
PERCENTAGEOFBASELINE    FLOAT
TIMELOCAL              VARCHAR
DATAAVAILABLE          BOOLEAN
STATION                VARCHAR
```

### CLEAN_DATA Table
```sql
PERCENTAGEOFBASELINE   FLOAT
TIMELOCAL              VARCHAR
DATE                   DATE
STATION                VARCHAR
```

### AGGREGATED_DATA Table
```sql
DATE                   DATE
STATION                VARCHAR
AVG_CROWDING           FLOAT
STD_CROWDING           FLOAT
SAMPLE_COUNT           INTEGER
CI_LOWER               FLOAT
CI_UPPER               FLOAT
```

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Standards

- Follow PEP 8 style guidelines
- Add docstrings to functions
- Include type hints where applicable
- Write descriptive commit messages

---

## License

This project is open source and available under the MIT License.

---

## Contact & Support

For questions, issues, or suggestions, please:

- Open an issue on GitHub
- Contact the project maintainer

---

## Useful Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [TFL API Documentation](https://tfl.gov.uk/info-for/open-data-users/)
- [Polars Documentation](https://docs.pola-rs.com/)

---

**Last Updated:** June 29, 2026  
**Maintained By:** YassineSdk

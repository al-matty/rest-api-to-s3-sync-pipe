# API to S3 Hourly Syncing and Backfilling + dbt Transformations

Self-hosted ELT pipeline for REST API → S3 → Data Warehouse, ready for orchestration with production-grade features, e.g. error code-based retry logic, S3 sync with backfilling, customizable logging, and an example dbt integration for transformations.

The pipeline consists of three layers: **Extraction** ([scripts/](scripts/)) fetches hourly event data from REST API, **Loading** uploads to S3 with duplicate prevention, and **Transformation** ([transform/](transform/)) uses dbt to create analytics-ready tables. Run extraction commands with the `--dev` flag to simulate S3 as local folder.

---

## Project Structure

```
├── scripts/      # Python extraction & loading
├── transform/    # dbt transformations (staging → intermediate)
├── data/         # Local JSONL files
└── logs/         # Execution logs
```

---

## Fetching Date Range

The pipeline will ensure data on the S3 bucket is complete in the time range from `DEFAULT_LOOKBACK_DAYS` to `DATA_AVAILABILITY_LAG_HOURS` hours ago. You can set these variables in [scripts/utils.py](scripts/utils.py):

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_LOOKBACK_DAYS` | `1` | Days to look back when no date range specified |
| `DATA_AVAILABILITY_LAG_HOURS` | `12` | API data availability buffer (hours) |

**Default behavior:** `python scripts/run.py fetch` retrieves ~25 hours of data ending ~12 hours ago (to account for data availability delays of the endpoint).

---

## Workflows

### `Fetching Workflow` - Fetching & Backfilling
- `python scripts/run.py fetch`
- **S3 data availability check** - Check S3 for continuous data, adjust query start date if found
- Generate required hourly files (start → end range)
- Get existing local files
- Calculate missing files (required files set minus locally available files set)
- Query API for missing hours
- Save as `data/YYYY-MM-DD_HH.jsonl`

![Fetching Workflow](img/diagram_fetching.png)


### `Syncing Workflow` - S3 syncing
- `python scripts/run.py sync`
- Get local files
- Get remote S3 files
- Remove local files already in S3 (prevent duplicates)
- Push remaining files to S3 (subfolder `python-import/`)
- Clean up local files after successful upload

![Syncing Workflow](img/diagram_syncing.png)

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt

# Optional: Install dbt for transformations
pip install dbt-snowflake  # or dbt-bigquery, dbt-postgres, dbt-duckdb
```

### 2. Configure credentials
Create `.env` file:
```env
# API credentials
AMP_API_KEY=your_api_key
AMP_SECRET_KEY=your_secret_key

# AWS S3 credentials
AWS_BUCKET_NAME=your-s3-bucket
AWS_REGION=eu-west-2
AWS_PYTHON_USER_ACCESS_KEY=your_aws_key
AWS_PYTHON_USER_SECRET_KEY=your_aws_secret
```

### 3. Run extraction
```bash
cd scripts
python run.py fetch      # Fetch data (last 1 day)
python run.py sync       # Upload to S3 and cleanup local files
python run.py all        # Complete pipeline (fetch + sync)
python run.py all --dev  # Dev mode: use local s3_dev/ folder (no AWS calls)
```

### 4. Run transformations (optional)
```bash
cd transform
dbt run                       # Run all transformations
dbt test                      # Test data quality
dbt docs serve                # View documentation
dbt run --select staging      # Run staging layer only
dbt run --select stg_events   # Run specific model

```

---


## File Format

**Hourly snapshots**: `data/2025-11-10_21.jsonl`
- One file per hour
- JSONL format (newline-delimited JSON)
- Uploaded to: `s3://bucket/python-import/2025-11-10_21.jsonl`

---

## Development Mode

Use `--dev` flag to avoid AWS API calls during development:
- S3 operations use local `s3_dev/` folder instead of AWS
- Perfect for testing without incurring AWS costs
- Simulates full S3 workflow locally

---

## Scheduling

### Extraction + Loading (Cron)
```bash
# Run hourly
0 * * * * cd /path/to/project && python scripts/run.py all
```

### Transformations (Cron)
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/project/transform && dbt run && dbt test
```

### GitHub Actions
```yaml
on:
  schedule:
    - cron: '0 * * * *'  # Extract hourly
    - cron: '0 2 * * *'  # Transform daily
```

---

## License

MIT

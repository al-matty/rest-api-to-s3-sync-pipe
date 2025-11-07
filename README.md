# Amplitude Analytics Data Exporter

A Python script that fetches event data from Amplitude's EU residency API server and stores it locally as JSON files. The tool is designed to export historical event data with robust retry logic for handling transient failures. Data can be subsequently loaded into Azure Blob Storage for long-term analytics and reporting.


## Value Proposition

- **Automated Data Extraction**: Eliminates manual export processes from Amplitude UI
- **Resilient & Reliable**: Built-in retry logic ensures data isn't lost due to transient API failures
- **EU GDPR Compliant**: Uses EU residency server for data sovereignty requirements
- **Cloud-Ready**: Prepared for Azure Blob Storage integration for scalable analytics
- **Audit Trail**: Comprehensive logging for compliance and debugging
- **Cost-Effective**: Lightweight Python solution with minimal infrastructure requirements

## Features

- **Automatic Daily Export**: Fetches previous day's events (00:00-23:00)
- **Resilient Retry Logic**: Handles server errors and rate limits with configurable retries
- **EU Compliance**: Uses Amplitude's EU residency server
- **Comprehensive Logging**: Timestamped logs for audit trails
- **Automatic Date Handling**: Calculates date ranges automatically
- **Error Handling**: Graceful handling of API failures with exponential backoff
- **Azure Integration Ready**: Configured for Azure Blob Storage (Germany West Central)

## Prerequisites

- Python 3.12 or higher
- Amplitude API credentials (API key and secret key)
- Internet connection to access Amplitude API

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd amplitude
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**:
   Create a `.env` file in the project root:
   ```env
   AMP_API_KEY=your_api_key_here
   AMP_SECRET_KEY=your_secret_key_here
   ```

## Configuration

### Environment Variables (`.env`)

| Variable | Description |
|----------|-------------|
| `AMP_API_KEY` | Your Amplitude API key |
| `AMP_SECRET_KEY` | Your Amplitude secret key |

### Script Configuration ([fetch_data.py](fetch_data.py))

| Parameter | Default | Description |
|-----------|---------|-------------|
| `log_outpath` | `"logs"` | Directory for log files |
| `log_level` | `logging.INFO` | Logging level |
| `data_outpath` | `"data"` | Directory for exported data |
| `data_outfile` | `"ampl_dump"` | Base name for data files |
| `max_attempts` | `5` | Maximum retry attempts |
| `delay_seconds` | `1` | Base delay between retries |

## Usage

Run the script to fetch yesterday's data:

```bash
python fetch_data.py
```

### What Happens:

1. Calculates the previous day's date range (T00 to T23)
2. Authenticates with Amplitude API using credentials from `.env`
3. Fetches event data from `https://analytics.eu.amplitude.com/api/2/export`
4. Retries automatically on failures (up to 5 attempts)
5. Extracts and decompresses the zip archive
6. Saves events to `data/YYYYMMDD_HHMM_ampl_dump.json`
7. Logs all operations to `logs/log_YYYYMMDD_HHMMSS.log`

### Retry Logic Diagram:

```
┌─────────────┐
│ API Request │
└──────┬──────┘
       │
       ▼
  ┌─────────┐
  │ Status? │
  └────┬────┘
       │
       ├─── 200 ────────────► Return Data ✓
       │
       ├─── 5xx ────► Wait 1s ──┐
       │                        │
       ├─── 429 ────► Wait 2s ──┤
       │                        │
       │                        ▼
       │                 ┌──────────────┐
       │                 │ Attempts < 5?│
       │                 └──────┬───────┘
       │                        │
       │                   Yes  │  No
       │                        │  │
       │                        │  └──► Exit(1) ✗
       │                        │
       │                        └──► Retry (Recursive)
       │
       └─── Other ──────────► Raise Error ✗
```

| Status Code | Wait Time | Action |
|------------|-----------|--------|
| 200 | - | Return data |
| 5xx | 1s | Retry (max 5) |
| 429 | 2s | Retry (max 5) |
| Other | - | Raise exception |


## Architecture

### Components

```
amplitude/
├── fetch_data.py           # Main application script
├── utils.py                # Utility functions (fetch, unzip, write)
├── logging_config.py       # Logging configuration
├── .env                    # API credentials (not in git)
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore patterns
├── data/                  # Exported JSON files (created at runtime)
└── logs/                  # Application logs (created at runtime)
```

### Main Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `fetch()` | [utils.py:32-81](utils.py#L32-L81) | Fetches data from Amplitude API with retry logic |
| `unzip()` | [utils.py:20-29](utils.py#L20-L29) | Extracts and decompresses event data |
| `write_to_local()` | [utils.py:83-92](utils.py#L83-L92) | Saves events to timestamped JSON file |
| `setup_logging()` | [logging_config.py](logging_config.py) | Configures logging with timestamps |

### Data Flow

```
1. Load .env credentials
   ↓
2. Calculate yesterday's date range
   ↓
3. Make API request to Amplitude
   ↓
4. [Retry Logic] Handle failures automatically
   ├→ 5xx errors: wait 1s, retry
   ├→ 429 errors: wait 2s, retry
   └→ max 5 attempts
   ↓
5. Receive raw zip bytes
   ↓
6. Unzip → Decompress gzip → Parse JSON lines
   ↓
7. Write to data/TIMESTAMP_ampl_dump.json
   ↓
8. Log success
```

## Output

### Data Files

**Location**: `data/` directory
**Format**: `YYYYMMDD_HHMM_ampl_dump.json`
**Example**: `20251107_0043_ampl_dump.json`

**Structure**: JSON array of event objects
```json
[
  {
    "event_type": "page_view",
    "user_id": "12345",
    "time": 1699286400000,
    ...
  }
]
```

### Log Files

**Location**: `logs/` directory
**Format**: `log_YYYYMMDD_HHMMSS.log`
**Example**: `log_20251107_004312.log`

**Log Entry Format**:
```
2025-11-07 00:43:12,345 - INFO - Data fetched successfully (status 200)
```


### Deployment Options

**1. Cron Job (Linux/macOS)**
```bash
# Run daily at 2 AM
0 2 * * * cd /path/to/amplitude && python fetch_data.py
```

**3. Cloud Schedulers**
- AWS EventBridge + Lambda
- Google Cloud Scheduler + Cloud Functions
- Azure Functions with Timer Trigger

**4. GitHub Actions**
```yaml
# .github/workflows/export.yml
on:
  schedule:
    - cron: '0 2 * * *'
```


## License

MIT-licensed, see LICENSE file.

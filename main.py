"""
Fetch and store Amplitude event data for a specified date range.
Data is fetched from the Amplitude EU residency server.
Data goes to the data/ directory as JSON files named with timestamps.
Logging is configured to output to a file in the logs/ directory.
"""

import logging
import os, sys
from dotenv import load_dotenv
from datetime import datetime, timedelta
from logging_config import setup_logging
from utils import fetch, get_local_files, query_difference, push_to_s3, write_hourly_snapshots


# Configuration
log_outpath = "logs"
log_level = logging.INFO    # or logging.ERROR
data_outpath = "data"
data_outfile = ""
max_attempts = 5
delay_seconds = 0.5
lookback_days = 1
url = "https://analytics.eu.amplitude.com/api/2/export"

# Load .env & set up logging
load_dotenv()
setup_logging(log_outpath, log_level)

# Get secrets from .env
s3_bucket = os.getenv("AWS_BUCKET_NAME")
api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

# Format the start and end time (YYYYMMDDTHH)
yesterday = datetime.now() - timedelta(days=1)
end_time = yesterday.strftime("%Y%m%dT23")
start_time = (yesterday - timedelta(days=lookback_days)).strftime("%Y%m%dT00")


print(end_time)
print(start_time)

# Fetch data & store locally
data = fetch(url, api_key, secret_key, delay_seconds, start_time, end_time, max_attempts)
write_hourly_snapshots(data, data_outpath)
sys.exit()


# Generate required hourly files for the lookback period
now = datetime.now()
start_dt = now - timedelta(days=lookback_days)
required_files = set()


# Create set of required hourly filenames (format: "2025-11-10_21")
current_dt = start_dt
while current_dt <= now:
    filename = current_dt.strftime("%Y-%m-%d_%H")
    required_files.add(filename)
    current_dt += timedelta(hours=1)


print(f"Required files: {len(required_files)} hourly files")

print("\n")
print(required_files)
sys.exit(1)


# Get existing local files
existing_files = get_local_files(data_outpath)
print(f"Existing files: {len(existing_files)} hourly files")

# Calculate difference (missing files)
missing_files = required_files - existing_files
print(f"Missing files: {len(missing_files)} hourly files")

if missing_files:
    print(f"\nMissing hours: {sorted(missing_files)}")



# Fetch only missing data
query_difference(
    missing_files=missing_files,
    url=url,
    api_key=api_key,
    secret_key=secret_key,
    delay_seconds=delay_seconds,
    max_attempts=max_attempts,
    data_outpath=data_outpath
)


push_to_s3(s3_bucket, data_outpath)
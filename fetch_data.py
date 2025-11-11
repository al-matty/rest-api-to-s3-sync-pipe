"""
Fetch and store Amplitude event data for a specified date range.
Data is fetched from the Amplitude EU residency server.
Data goes to the data/ directory as JSON files named with timestamps.
Logging is configured to output to a file in the logs/ directory.
"""

import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from logging_config import setup_logging
from utils import fetch, write_hourly_snapshots

# Load .env file
load_dotenv()

# Configuration
log_outpath = "logs"
log_level = logging.INFO    # or logging.ERROR
data_outpath = "data"
data_outfile = ""
max_attempts = 5
delay_seconds = 1
lookback_days = 1

# Set up logging
setup_logging(log_outpath, log_level)

# Get secrets from .env
api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

# Format the start and end time (YYYYMMDDTHH)
now = datetime.now()
start_time = (now - timedelta(days=lookback_days)).strftime("%Y%m%dT%H")
end_time = now.strftime("%Y%m%dT%H")

# API endpoint is the EU residency server
url = "https://analytics.eu.amplitude.com/api/2/export"

# Fetch data
data = fetch(
    url,
    api_key,
    secret_key,
    delay_seconds,
    start_time,
    end_time,
    max_attempts
    )

# Unzip & write hourly snapshots to local
write_hourly_snapshots(data, data_outpath)

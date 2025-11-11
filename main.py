"""
Fetch and store Amplitude event data for a specified date range.
Data is fetched from the Amplitude EU residency server.
Data goes to the data/ directory as JSON files named with timestamps.
Logging is configured to output to a file in the logs/ directory.

NOTE: This is the legacy main.py file. For CLI usage, use run.py instead:
    python run.py fetch    # Fetch data
    python run.py sync     # Sync to S3
    python run.py all      # Complete pipeline
"""

import logging
from datetime import datetime, timedelta
from logging_config import setup_logging
from utils import complete_workflow


# Configuration
log_outpath = "logs"
log_level = logging.INFO

# Setup logging
setup_logging(log_outpath, log_level)

# Calculate date range (default: last 7 days to now)
lookback_days = 7
now = datetime.now()
start_dt = now - timedelta(days=lookback_days)

# Format dates for API (YYYYMMDDTHH)
start_time = start_dt.strftime("%Y%m%dT%H")
end_time = now.strftime("%Y%m%dT%H")

print("Running complete workflow for date range:")
print(f"  Start: {start_time}")
print(f"  End:   {end_time}")

# Run complete workflow (fetch + sync)
complete_workflow(start_time, end_time)
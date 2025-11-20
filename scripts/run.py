#!/usr/bin/env python3
"""
CLI interface for pipeline:

    python run.py fetch              - Fetch data from Amplitude API and save locally
    python run.py sync               - Sync local files to S3 and cleanup
    python run.py all                - Run complete pipeline (fetch + sync)

Optional arguments:
    --start-date YYYYMMDDTHH         - Start date for data export (default: 1 day ago from adjusted end)
    --end-date YYYYMMDDTHH           - End date for data export (default: 12 hours ago)
    --dev                            - Development mode: use local s3_dev/ folder instead of AWS S3
"""

import argparse
import sys
from datetime import datetime, timedelta
from scripts.logging_config import setup_logging
from scripts.utils import fetch_workflow, sync_workflow, complete_workflow, DEFAULT_LOOKBACK_DAYS, DATA_AVAILABILITY_LAG_HOURS

# Setup logging
setup_logging()


def get_default_dates(lookback_days=DEFAULT_LOOKBACK_DAYS):
    """Get default start and end dates."""
    now = datetime.utcnow()

    # Subtract lag hours from 'now' to ensure data is available
    adjusted_now = now - timedelta(hours=DATA_AVAILABILITY_LAG_HOURS)
    start_dt = adjusted_now - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y%m%dT%H")
    end_date = adjusted_now.strftime("%Y%m%dT%H")

    return start_date, end_date


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Amplitude Data Export Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py fetch                           # Fetch last 1 day
  python run.py fetch --start-date 20251110T00 --end-date 20251110T23
  python run.py sync                            # Sync local files to S3
  python run.py all                             # Fetch + sync
  python run.py all --dev                       # Dev mode: use local s3_dev/ folder
        """
    )

    parser.add_argument(
        "command",
        choices=["fetch", "sync", "all"],
        help="Command to execute: fetch (get data), sync (upload to S3), or all (both)"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (format: YYYYMMDDTHH, e.g., 20251110T00). Default: 1 day before adjusted end"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (format: YYYYMMDDTHH, e.g., 20251110T23). Default: 12 hours ago (accounts for data lag)"
    )

    parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use local s3_dev/ folder instead of AWS S3 (saves API calls)"
    )

    args = parser.parse_args()

    # Get dates (use defaults if not provided)
    default_start, default_end = get_default_dates()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    # Show mode
    if args.dev:
        print("\n🔧 DEV MODE: Using local s3_dev/ folder instead of AWS S3")

    # Execute command
    try:
        if args.command == "fetch":
            print(f"Running FETCH workflow: {start_date} to {end_date}")
            fetch_workflow(start_date, end_date, dev_mode=args.dev)

        elif args.command == "sync":
            print("Running SYNC workflow")
            sync_workflow(dev_mode=args.dev)

        elif args.command == "all":
            print(f"Running COMPLETE workflow: {start_date} to {end_date}")
            complete_workflow(start_date, end_date, dev_mode=args.dev)

        print("\n✓ Command completed successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

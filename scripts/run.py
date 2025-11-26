#!/usr/bin/env python3
"""
CLI interface for data pipeline.

Amplitude (default):
    python run.py fetch                           # Default → amplitude fetch
    python run.py amplitude fetch                 # Explicit amplitude
    python run.py amplitude sync
    python run.py amplitude all --dev

Mailchimp:
    python run.py mailchimp test                  # Run API feasibility test
    python run.py mailchimp fetch                 # Fetch last 14 days (default)
    python run.py mailchimp fetch --lookback-days 30
    python run.py mailchimp sync
    python run.py mailchimp all --dev
"""

import argparse
import sys
from datetime import datetime, timedelta
from scripts.logging_config import setup_logging
from scripts.utils import (
    fetch_workflow as amplitude_fetch_workflow,
    sync_workflow as amplitude_sync_workflow,
    complete_workflow as amplitude_complete_workflow,
    DEFAULT_LOOKBACK_DAYS,
    DATA_AVAILABILITY_LAG_HOURS,
)
from scripts.mailchimp_utils import (
    fetch_workflow as mailchimp_fetch_workflow,
    sync_workflow as mailchimp_sync_workflow,
    complete_workflow as mailchimp_complete_workflow,
    DEFAULT_CAMPAIGN_LOOKBACK_DAYS,
)

# Setup logging
setup_logging()


def get_amplitude_default_dates(lookback_days=DEFAULT_LOOKBACK_DAYS):
    """Get default start and end dates for Amplitude."""
    now = datetime.utcnow()
    adjusted_now = now - timedelta(hours=DATA_AVAILABILITY_LAG_HOURS)
    start_dt = adjusted_now - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y%m%dT%H")
    end_date = adjusted_now.strftime("%Y%m%dT%H")
    return start_date, end_date


def run_amplitude(args):
    """Execute Amplitude commands."""
    default_start, default_end = get_amplitude_default_dates()
    start_date = args.start_date or default_start
    end_date = args.end_date or default_end

    if args.dev:
        print("\nDEV MODE: Using local s3_dev/ folder instead of AWS S3")

    if args.command == "fetch":
        print(f"Running Amplitude FETCH workflow: {start_date} to {end_date}")
        amplitude_fetch_workflow(start_date, end_date, dev_mode=args.dev)

    elif args.command == "sync":
        print("Running Amplitude SYNC workflow")
        amplitude_sync_workflow(dev_mode=args.dev)

    elif args.command == "all":
        print(f"Running Amplitude COMPLETE workflow: {start_date} to {end_date}")
        amplitude_complete_workflow(start_date, end_date, dev_mode=args.dev)


def run_mailchimp(args):
    """Execute Mailchimp commands."""
    if args.dev:
        print("\nDEV MODE: Using local s3_dev/ folder instead of AWS S3")

    if args.command == "test":
        # Import and run the test script
        from scripts.mailchimp_test import run_tests
        print("Running Mailchimp API feasibility test...")
        results = run_tests()
        if not all(results.values()):
            sys.exit(1)

    elif args.command == "fetch":
        lookback = args.lookback_days
        print(f"Running Mailchimp FETCH workflow (last {lookback} days)")
        mailchimp_fetch_workflow(lookback_days=lookback)

    elif args.command == "sync":
        print("Running Mailchimp SYNC workflow")
        mailchimp_sync_workflow(dev_mode=args.dev)

    elif args.command == "all":
        lookback = args.lookback_days
        print(f"Running Mailchimp COMPLETE workflow (last {lookback} days)")
        mailchimp_complete_workflow(lookback_days=lookback, dev_mode=args.dev)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Export Pipeline (Amplitude & Mailchimp)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Amplitude (default when no source specified)
  python run.py fetch                           # Same as: amplitude fetch
  python run.py amplitude fetch --start-date 20251110T00 --end-date 20251110T23
  python run.py amplitude sync
  python run.py amplitude all --dev

  # Mailchimp
  python run.py mailchimp test                  # Test API connectivity
  python run.py mailchimp fetch                 # Fetch last 14 days
  python run.py mailchimp fetch --lookback-days 30
  python run.py mailchimp all --dev
        """
    )

    subparsers = parser.add_subparsers(dest="source", help="Data source")

    # ==========================================================================
    # Amplitude subparser
    # ==========================================================================
    amp_parser = subparsers.add_parser("amplitude", help="Amplitude data export")
    amp_parser.add_argument(
        "command",
        choices=["fetch", "sync", "all"],
        help="Command: fetch (get data), sync (upload to S3), or all (both)"
    )
    amp_parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (format: YYYYMMDDTHH). Default: 1 day before end"
    )
    amp_parser.add_argument(
        "--end-date",
        type=str,
        help="End date (format: YYYYMMDDTHH). Default: 12 hours ago"
    )
    amp_parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use local s3_dev/ folder"
    )

    # ==========================================================================
    # Mailchimp subparser
    # ==========================================================================
    mc_parser = subparsers.add_parser("mailchimp", help="Mailchimp data export")
    mc_parser.add_argument(
        "command",
        choices=["test", "fetch", "sync", "all"],
        help="Command: test (API test), fetch, sync, or all"
    )
    mc_parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_CAMPAIGN_LOOKBACK_DAYS,
        help=f"Days to look back for campaigns (default: {DEFAULT_CAMPAIGN_LOOKBACK_DAYS})"
    )
    mc_parser.add_argument(
        "--dev",
        action="store_true",
        help="Development mode: use local s3_dev/ folder"
    )

    # Handle default: if first arg is a valid Amplitude command (not a subparser),
    # prepend "amplitude" to make it the default
    if len(sys.argv) > 1 and sys.argv[1] in ["fetch", "sync", "all"]:
        sys.argv.insert(1, "amplitude")

    # Parse arguments
    args = parser.parse_args()

    # If no source specified, show help
    if args.source is None:
        parser.print_help()
        sys.exit(1)

    # Execute
    try:
        if args.source == "amplitude":
            run_amplitude(args)
        elif args.source == "mailchimp":
            run_mailchimp(args)

        print("\nCommand completed successfully!")
        sys.exit(0)

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

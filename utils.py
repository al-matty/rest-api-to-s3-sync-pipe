"""
Utility functions for the Amplitude data export pipeline.
"""

import json
import logging
import requests
import os
import sys
import time
import io
import gzip
import zipfile
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

#from logging_config import setup_logging
#setup_logging()

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_PYTHON_USER_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_PYTHON_USER_SECRET_KEY"),
)


def fetch(
    url: str,
    api_key: str,
    secret_key: str,
    delay_seconds: int,
    start: str,
    end: str,
    max_attempts: int = 5,
    n_attempts: int = 0,
) -> bytes:
    """Fetch data from Amplitude API. Retry on server errors and rate limits."""

    n_attempts += 1
    print(f"Fetch attempt {n_attempts}...")

    if n_attempts > max_attempts:
        logger.error(f"Max fetch attempts reached ({n_attempts-1}/{max_attempts}). Exiting.")
        print(f"Tried fetching unsuccessfully {n_attempts-1} times. Exiting.")
        sys.exit(1)

    logger.info(f"Fetching data from {url}")
    response = requests.get(
        url, params={"start": start, "end": end}, auth=(api_key, secret_key)
    )
    code = response.status_code

    # Recursive retry logic

    # 200: Return data
    if code == 200:
        logger.info(f"Data fetched successfully (status {code})")
        return response.content

    # 5xx: Recursively call again after a delay
    elif str(code).startswith("5"):
        logger.error(f"Got status code {code}. Retrying in {delay_seconds}s ...")
        time.sleep(delay_seconds)
        return fetch(url, api_key, secret_key, delay_seconds, start, end, max_attempts, n_attempts)

    # 429: Rate limits: Recursively call again after a longer delay
    elif code == 429:
        logger.error(f"Got status code {code}. Retrying in {delay_seconds}s ...")
        time.sleep(delay_seconds * 2)
        return fetch(url, api_key, secret_key, delay_seconds, start, end, max_attempts, n_attempts)

    # any other code: Log and raise exception (quit)
    else:
        logger.error(f"Unhandled API error {code}: {response.text}")
        raise Exception(f"Error {response.status_code}: {response.text}")


def write_hourly_snapshots(data: bytes, outpath: str) -> None:
    """Extract and write one file per hour."""
    os.makedirs(outpath, exist_ok=True)
    
    file_count = 0
    logger.info(f"Writing hourly snapshots to {outpath}")

    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for filename in z.namelist():
            content = gzip.decompress(z.read(filename)).decode()

            # Clean file names: "100011471_2025-11-09_21#xyz.json.gz" -> "2025-11-09_21"
            base_name = filename.split('/')[-1]  # Remove directory path if present
            base_name = base_name.replace('.json.gz', '')  # Remove extension
            base_name = base_name.split('#')[0]  # Remove hash suffix
            if '_' in base_name and base_name.split('_')[0].isdigit():
                hour_key = '_'.join(base_name.split('_')[1:])  # Remove first segment (project ID)
            else:
                hour_key = base_name

            with open(f"{outpath}/{hour_key}.jsonl", 'a') as f:  # ← Use JSONL format
                f.write(content)  # jsonl = newline-delimited
                file_count += 1
    
    logger.info(f"{file_count} Files saved")


def get_local_files(data_dir: str = "data") -> set[str]:
    """Get set of existing hourly file names (without .jsonl extension)."""
    if not os.path.exists(data_dir):
        return set()

    files = [
        f.replace('.jsonl', '')
        for f in os.listdir(data_dir)
        if f.endswith('.jsonl')
    ]
    logger.info(f"Found {len(files)} existing files in {data_dir}")
    return set(files)


def filename_to_timestamp(filename: str) -> str:
    """Convert filename format to API timestamp format.

    Args:
        filename: Format like "2025-11-10_21"

    Returns:
        API timestamp format like "20251110T21"
    """
    # Parse: "2025-11-10_21" -> "20251110T21"
    date_part, hour_part = filename.rsplit('_', 1)
    clean_date = date_part.replace('-', '')
    return f"{clean_date}T{hour_part}"


def query_difference(
    missing_files: set[str],
    url: str,
    api_key: str,
    secret_key: str,
    delay_seconds: float,
    max_attempts: int,
    data_outpath: str
) -> None:
    """Fetch and save data for missing hourly files.

    Args:
        missing_files: Set of filenames like {"2025-11-10_21", "2025-11-10_22"}
        url: Amplitude API endpoint
        api_key: Amplitude API key
        secret_key: Amplitude secret key
        delay_seconds: Retry delay
        max_attempts: Maximum fetch attempts
        data_outpath: Output directory for data files
    """
    if not missing_files:
        logger.info("No missing files to fetch")
        print("All files already exist. Nothing to fetch.")
        return

    logger.info(f"Fetching {len(missing_files)} missing hourly files")
    print(f"Fetching {len(missing_files)} missing hours...")

    for filename in sorted(missing_files):
        timestamp = filename_to_timestamp(filename)
        logger.info(f"Fetching data for {filename} (timestamp: {timestamp})")
        print(f"  Fetching {filename}...")

        # Fetch single hour (start = end for hourly data)
        data = fetch(
            url=url,
            api_key=api_key,
            secret_key=secret_key,
            delay_seconds=delay_seconds,
            start=timestamp,
            end=timestamp,
            max_attempts=max_attempts
        )

        # Write the hourly snapshot
        write_hourly_snapshots(data, data_outpath)

    logger.info(f"Successfully fetched {len(missing_files)} missing files")
    print(f"✓ Fetched {len(missing_files)} missing hours")


def generate_required_files(start_date: str, end_date: str) -> set[str]:
    """Generate complete list of hourly timestamps from start to end.

    Args:
        start_date: Start datetime string (format: "YYYYMMDDTHH" or "YYYY-MM-DD HH")
        end_date: End datetime string (format: "YYYYMMDDTHH" or "YYYY-MM-DD HH")

    Returns:
        Set of filenames like {"2025-11-10_21", "2025-11-10_22"}
    """
    from datetime import timedelta

    # Parse start and end dates - support multiple formats
    if 'T' in start_date:
        # Format: "20251110T21"
        start_dt = datetime.strptime(start_date, "%Y%m%dT%H")
    else:
        # Format: "2025-11-10 21" or similar
        start_dt = datetime.strptime(start_date.replace('-', '').replace(' ', 'T'), "%Y%m%dT%H")

    if 'T' in end_date:
        end_dt = datetime.strptime(end_date, "%Y%m%dT%H")
    else:
        end_dt = datetime.strptime(end_date.replace('-', '').replace(' ', 'T'), "%Y%m%dT%H")

    # Generate hourly timestamps
    required = set()
    current = start_dt
    while current <= end_dt:
        # Format: "2025-11-10_21"
        filename = current.strftime("%Y-%m-%d_%H")
        required.add(filename)
        current += timedelta(hours=1)

    logger.info(f"Generated {len(required)} required hourly files from {start_date} to {end_date}")
    return required


def get_s3_files(bucket: str, prefix: str = "python-import/") -> set[str]:
    """Get set of existing files in S3 bucket (without .jsonl extension).

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix/folder path (default: "python-import/")

    Returns:
        Set of filenames like {"2025-11-10_21", "2025-11-10_22"}
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if 'Contents' not in response:
            logger.info(f"No files found in s3://{bucket}/{prefix}")
            return set()

        files = [
            obj['Key'].replace(prefix, '').replace('.jsonl', '')
            for obj in response['Contents']
            if obj['Key'].endswith('.jsonl')
        ]

        logger.info(f"Found {len(files)} files in S3 bucket {bucket}")
        return set(files)

    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        raise e


def cleanup_local_files(data_dir: str = "data", files: set[str] | None = None) -> None:
    """Delete local files after successful S3 upload.

    Args:
        data_dir: Directory containing data files (default: "data")
        files: Specific set of filenames to delete (without .jsonl extension).
               If None, deletes all .jsonl files in the directory.
    """
    if not os.path.exists(data_dir):
        logger.warning(f"Data directory {data_dir} does not exist")
        return

    # Get files to delete
    if files is None:
        files_to_delete = [f for f in os.listdir(data_dir) if f.endswith('.jsonl')]
    else:
        files_to_delete = [f"{filename}.jsonl" for filename in files]

    if not files_to_delete:
        logger.info("No files to cleanup")
        return

    logger.info(f"Cleaning up {len(files_to_delete)} local files")
    print(f"Cleaning up {len(files_to_delete)} local files...")

    deleted_count = 0
    for f in files_to_delete:
        file_path = os.path.join(data_dir, f)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted {file_path}")
                deleted_count += 1
        except Exception as e:
            logger.error(f"Error deleting {file_path}: {e}")

    logger.info(f"Successfully deleted {deleted_count} files")
    print(f"✓ Cleaned up {deleted_count} files")


def push_to_s3(bucket, data_dir: str = "data") -> None:
    """Upload all JSONL files from data directory to S3."""

    # Get all local JSONL files
    files = [f for f in os.listdir(data_dir) if f.endswith('.jsonl')]
    logger.info(f"Got {len(files)} JSONL files from local.")
    print(f"Uploading {len(files)} files to S3 bucket {bucket}...")

    # Upload each file
    for f in files:
        file_path = os.path.join(data_dir, f)
        try:
            s3_client.upload_file(file_path, bucket, f"python-import/{f}")
            logger.info(f"Uploaded {f} to s3://{bucket}/{f}")
        except Exception as e:
            logger.error(f"Error uploading {f}: {e}")
            raise e


# ============================================================================
# WORKFLOW FUNCTIONS
# ============================================================================

def fetch_workflow(start_date: str, end_date: str) -> None:
    """Fetch workflow: Query Amplitude API and save hourly snapshots locally.

    This workflow:
    1. Generates required hourly files from start_date to end_date
    2. Gets existing local files
    3. Calculates missing files (required - local)
    4. Fetches missing data from Amplitude API
    5. Saves as hourly JSONL snapshots

    Args:
        start_date: Start datetime (format: "YYYYMMDDTHH" or "YYYY-MM-DD_HH")
        end_date: End datetime (format: "YYYYMMDDTHH" or "YYYY-MM-DD_HH")
    """
    logger.info("=== Starting FETCH workflow ===")
    print("\n=== FETCH WORKFLOW ===")

    # Load environment variables
    api_key = os.getenv("AMP_API_KEY")
    secret_key = os.getenv("AMP_SECRET_KEY")
    url = "https://analytics.eu.amplitude.com/api/2/export"
    data_dir = "data"
    delay_seconds = 3.0
    max_attempts = 5

    # 1. Generate required files
    print(f"\n1. Generating required files from {start_date} to {end_date}...")
    required_files = generate_required_files(start_date, end_date)
    print(f"   → {len(required_files)} hourly files required")

    # 2. Get existing local files
    print("\n2. Checking existing local files...")
    local_files = get_local_files(data_dir)
    print(f"   → {len(local_files)} files already exist locally")

    # 3. Calculate missing files
    missing_files = required_files - local_files
    print("\n3. Calculating missing files...")
    print(f"   → {len(missing_files)} files need to be fetched")

    if not missing_files:
        print("\n✓ All files already exist. Nothing to fetch.")
        logger.info("=== FETCH workflow completed (no missing files) ===")
        return

    # 4. Fetch missing data
    print(f"\n4. Fetching {len(missing_files)} missing hourly snapshots...")
    query_difference(
        missing_files=missing_files,
        url=url,
        api_key=api_key,
        secret_key=secret_key,
        delay_seconds=delay_seconds,
        max_attempts=max_attempts,
        data_outpath=data_dir
    )

    print("\n✓ FETCH workflow completed successfully!")
    logger.info("=== FETCH workflow completed successfully ===")


def sync_workflow() -> None:
    """S3 sync workflow: Upload local files to S3 and cleanup.

    This workflow:
    1. Gets existing local files
    2. Gets remote S3 files
    3. Removes local files that already exist in S3 (prevent duplicates)
    4. Pushes remaining local files to S3
    5. Cleans up local files after successful upload

    Requires AWS credentials in .env file.
    """
    logger.info("=== Starting SYNC workflow ===")
    print("\n=== SYNC WORKFLOW ===")

    # Load environment variables
    bucket = os.getenv("AWS_BUCKET_NAME")
    data_dir = "data"

    # 1. Get local files
    print("\n1. Checking local files...")
    local_files = get_local_files(data_dir)
    print(f"   → {len(local_files)} files found locally")

    if not local_files:
        print("\n✓ No local files to sync.")
        logger.info("=== SYNC workflow completed (no local files) ===")
        return

    # 2. Get S3 files
    print("\n2. Checking S3 bucket...")
    s3_files = get_s3_files(bucket)
    print(f"   → {len(s3_files)} files already in S3")

    # 3. Remove overlap from local (files already in S3)
    overlap = local_files & s3_files
    if overlap:
        print(f"\n3. Removing {len(overlap)} files that already exist in S3...")
        cleanup_local_files(data_dir, overlap)
        local_files = local_files - overlap
    else:
        print("\n3. No overlap found - all local files are new")

    if not local_files:
        print("\n✓ All files already in S3. Nothing to upload.")
        logger.info("=== SYNC workflow completed (no new files) ===")
        return

    # 4. Push to S3
    print(f"\n4. Pushing {len(local_files)} files to S3...")
    push_to_s3(bucket, data_dir)

    # 5. Cleanup local files
    print("\n5. Cleaning up local files after successful upload...")
    cleanup_local_files(data_dir)

    print("\n✓ SYNC workflow completed successfully!")
    logger.info("=== SYNC workflow completed successfully ===")


def complete_workflow(start_date: str, end_date: str) -> None:
    """Complete workflow: Fetch data and sync to S3.

    This runs both fetch and sync workflows in sequence:
    1. Fetch missing data from Amplitude API
    2. Sync local files to S3 and cleanup

    Args:
        start_date: Start datetime (format: "YYYYMMDDTHH" or "YYYY-MM-DD_HH")
        end_date: End datetime (format: "YYYYMMDDTHH" or "YYYY-MM-DD_HH")
    """
    logger.info("=== Starting COMPLETE workflow (fetch + sync) ===")
    print("\n=== COMPLETE WORKFLOW (FETCH + SYNC) ===")

    # Run fetch workflow
    fetch_workflow(start_date, end_date)

    # Run sync workflow
    sync_workflow()

    print("\n✓ COMPLETE workflow finished!")
    logger.info("=== COMPLETE workflow finished successfully ===")

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

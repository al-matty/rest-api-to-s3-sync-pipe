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

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_USER_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_USER_SECRET_ACCESS_KEY"),
)


def unzip(data: bytes) -> list[dict]:
    """Extract and parse gzipped JSON from zip bytes."""
    logger.info("Unzipping and parsing data")
    events = []
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for filename in z.namelist():
            content = gzip.decompress(z.read(filename)).decode()
            events.extend([json.loads(line) for line in content.splitlines()])
    logger.info(f"Parsed {len(events)} events")
    return events


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


def write_to_local(events: list[dict], outpath: str, outfile: str) -> None:
    """Write events to local JSON file with timestamp."""
    os.makedirs(outpath, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{outpath}/{timestamp}_{outfile}.json"
    logger.info(f"Writing {len(events)} events to {filename}")
    with open(filename, "w") as f:
        json.dump(events, f, indent=2)
    logger.info("File saved successfully")
    print(f"Saved to {filename}")


def write_hourly_snapshots(data: bytes, outpath: str) -> None:
    """Extract and write one file per hour."""
    os.makedirs(outpath, exist_ok=True)
    
    file_count = 0
    logger.info(f"Writing hourly snapshots to {outpath}")

    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for filename in z.namelist():
            content = gzip.decompress(z.read(filename)).decode()
            hour_key = filename.replace('.json.gz', '').split('#')[0].split('/')[-1]

            with open(f"{outpath}/{hour_key}.jsonl", 'a') as f:  # ← Use JSONL format
                f.write(content)  # Already newline-delimited
                file_count += 1
    
    logger.info(f"{file_count} Files saved")


def push_to_s3(data_dir: str = "data") -> None:
    """Upload all JSONL files from data directory to S3."""
    bucket = os.getenv("BUCKET")

    try:
        # Test S3 connection
        buckets_list = s3_client.list_buckets()
        logger.info(f"Connected to S3 successfully (seeing {len(buckets_list['Buckets'])} buckets).")
    except Exception:
        error_msg = "Could not list S3 buckets. Check your AWS credentials."
        print(error_msg)
        logger.error(error_msg)
        sys.exit(1)

    # Get all JSON files
    files = [f for f in os.listdir(data_dir) if f.endswith('.jsonl')]
    logger.info(f"Got {len(files)} JSONL files from local.")
    print(f"Uploading {len(files)} files to S3 bucket {bucket}...")

    # Upload each file
    for f in files:
        file_path = os.path.join(data_dir, f)
        try:
            s3_client.upload_file(file_path, bucket, f)
            logger.info(f"Uploaded {f} to s3://{bucket}/{f}")
        except Exception as e:
            logger.error(f"Error uploading {f}: {e}")
            raise e


push_to_s3()
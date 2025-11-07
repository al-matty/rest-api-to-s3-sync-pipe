"""
Utility functions for the Amplitude data export pipeline.
Contains functions for fetching, unzipping, and writing data.
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
from datetime import datetime

logger = logging.getLogger(__name__)


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
        logger.error(f"Max fetch attempts reached ({n_attempts}/{max_attempts}). Exiting.")
        print(f"Tried fetching unsuccessfully {n_attempts} times. Exiting.")
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

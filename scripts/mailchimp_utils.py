"""
Mailchimp extraction utility functions.

Fetches data from Mailchimp Marketing API for the 4-table simplified schema:
1. CAMPAIGNS - Campaign metadata
2. CAMPAIGN_REPORTS - Aggregate metrics
3. EMAIL_SUBSCRIBERS - Subscriber master list
4. EMAIL_CLICKS - Granular click events
"""

import logging
import os
import json
import time
import requests
import boto3
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Configuration
DEFAULT_CAMPAIGN_LOOKBACK_DAYS = 14
MAILCHIMP_DATA_DIR = "data/mailchimp"
DEV_S3_DIR = "s3_dev"
S3_PREFIX = "mailchimp-python"

# Initialize S3 client (reuse from Amplitude utils pattern)
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_PYTHON_USER_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_PYTHON_USER_SECRET_KEY"),
)


# =============================================================================
# AUTHENTICATION
# =============================================================================

def get_mailchimp_config() -> tuple[str, str]:
    """Get Mailchimp API configuration from environment.

    Returns: (base_url, api_key)
    """
    api_key = os.getenv("MAILCHIMP_API_KEY")
    if not api_key:
        raise ValueError("MAILCHIMP_API_KEY not set in .env")

    # Extract data center from API key suffix (e.g., "xxx-us2" -> "us2")
    dc = api_key.split("-")[-1]
    base_url = f"https://{dc}.api.mailchimp.com/3.0"

    return base_url, api_key


def _make_request(endpoint: str, params: dict | None = None, max_retries: int = 3) -> dict:
    """Make authenticated request to Mailchimp API with retry logic."""
    base_url, api_key = get_mailchimp_config()
    url = f"{base_url}{endpoint}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, auth=("anystring", api_key), params=params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = (attempt + 1) * 5
                logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                raise Exception(f"API error {response.status_code}: {response.text}")

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Request failed, retrying: {e}")
                time.sleep(2)
            else:
                raise

    raise Exception(f"Max retries exceeded for {endpoint}")


# =============================================================================
# ENTITY FETCHING
# =============================================================================

def fetch_campaigns(lookback_days: int = DEFAULT_CAMPAIGN_LOOKBACK_DAYS, status: str = "sent") -> list[dict]:
    """Fetch campaigns sent within the lookback period.

    Args:
        lookback_days: Number of days to look back for campaigns
        status: Campaign status filter (default: "sent")

    Returns: List of campaign dicts with flattened structure
    """
    logger.info(f"Fetching campaigns from last {lookback_days} days...")
    print(f"   Fetching campaigns (last {lookback_days} days)...")

    cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
    campaigns = []
    offset = 0
    count = 100

    while True:
        data = _make_request(
            "/campaigns",
            params={
                "count": count,
                "offset": offset,
                "status": status,
                "sort_field": "send_time",
                "sort_dir": "DESC"
            }
        )

        batch = data.get("campaigns", [])
        if not batch:
            break

        for c in batch:
            send_time = c.get("send_time")
            if send_time:
                send_dt = datetime.fromisoformat(send_time.replace("Z", "+00:00").replace("+00:00", ""))
                if send_dt < cutoff_date:
                    # Stop fetching - we've gone past the lookback window
                    logger.info(f"Reached campaigns older than {lookback_days} days, stopping")
                    break

            # Flatten nested structure for easier warehouse loading
            campaigns.append({
                "campaign_id": c.get("id"),
                "list_id": c.get("recipients", {}).get("list_id"),
                "type": c.get("type"),
                "status": c.get("status"),
                "subject_line": c.get("settings", {}).get("subject_line"),
                "from_name": c.get("settings", {}).get("from_name"),
                "from_email": c.get("settings", {}).get("reply_to"),
                "send_time": c.get("send_time"),
                "emails_sent": c.get("emails_sent"),
            })
        else:
            # Inner loop didn't break, continue pagination
            offset += count
            continue

        # Inner loop broke (hit cutoff), stop outer loop
        break

    logger.info(f"Fetched {len(campaigns)} campaigns")
    print(f"   Found {len(campaigns)} campaigns")
    return campaigns


def fetch_campaign_reports(campaign_ids: list[str]) -> list[dict]:
    """Fetch aggregate reports for campaigns.

    Args:
        campaign_ids: List of campaign IDs to fetch reports for

    Returns: List of report dicts with flattened structure
    """
    logger.info(f"Fetching reports for {len(campaign_ids)} campaigns...")
    print(f"   Fetching reports for {len(campaign_ids)} campaigns...")

    reports = []

    for i, campaign_id in enumerate(campaign_ids):
        if (i + 1) % 10 == 0:
            print(f"      Progress: {i + 1}/{len(campaign_ids)}")

        try:
            data = _make_request(f"/reports/{campaign_id}")

            reports.append({
                "campaign_id": data.get("id"),
                "emails_sent": data.get("emails_sent"),
                "unique_opens": data.get("opens", {}).get("unique_opens"),
                "opens_total": data.get("opens", {}).get("opens_total"),
                "open_rate": data.get("opens", {}).get("open_rate"),
                "unique_clicks": data.get("clicks", {}).get("unique_clicks"),
                "clicks_total": data.get("clicks", {}).get("clicks_total"),
                "click_rate": data.get("clicks", {}).get("click_rate"),
                "unsubscribed": data.get("unsubscribed"),
            })
        except Exception as e:
            logger.warning(f"Failed to fetch report for {campaign_id}: {e}")

    logger.info(f"Fetched {len(reports)} reports")
    print(f"   Fetched {len(reports)} reports")
    return reports


def fetch_subscribers(list_id: str, since_last_changed: str | None = None) -> list[dict]:
    """Fetch subscribers from a list with pagination.

    Args:
        list_id: Mailchimp list/audience ID
        since_last_changed: Only fetch subscribers changed since this timestamp (ISO format)

    Returns: List of subscriber dicts with flattened structure
    """
    logger.info(f"Fetching subscribers for list {list_id}...")
    print(f"   Fetching subscribers (list: {list_id})...")

    subscribers = []
    offset = 0
    count = 500  # Mailchimp allows up to 1000

    params = {"count": count, "offset": offset}
    if since_last_changed:
        params["since_last_changed"] = since_last_changed
        logger.info(f"Incremental fetch: since {since_last_changed}")
        print(f"      (incremental: since {since_last_changed})")

    while True:
        params["offset"] = offset
        data = _make_request(f"/lists/{list_id}/members", params=params)

        batch = data.get("members", [])
        if not batch:
            break

        for m in batch:
            subscribers.append({
                "subscriber_hash": m.get("id"),
                "list_id": list_id,
                "email_address": m.get("email_address"),
                "status": m.get("status"),
                "timestamp_opt": m.get("timestamp_opt"),
                "last_changed": m.get("last_changed"),
                "merge_fields": json.dumps(m.get("merge_fields", {})),
            })

        offset += count

        # Progress indicator for large lists
        total = data.get("total_items", 0)
        if total > 0 and offset % 2000 == 0:
            print(f"      Progress: {min(offset, total)}/{total}")

    logger.info(f"Fetched {len(subscribers)} subscribers")
    print(f"   Fetched {len(subscribers)} subscribers")
    return subscribers


def fetch_email_clicks(campaign_ids: list[str]) -> list[dict]:
    """Fetch granular click events for campaigns.

    For each campaign:
    1. Get all URLs clicked (click-details)
    2. For each URL with clicks, get individual clicker data

    Args:
        campaign_ids: List of campaign IDs to fetch clicks for

    Returns: List of click event dicts
    """
    logger.info(f"Fetching clicks for {len(campaign_ids)} campaigns...")
    print(f"   Fetching clicks for {len(campaign_ids)} campaigns...")

    all_clicks = []

    for i, campaign_id in enumerate(campaign_ids):
        if (i + 1) % 5 == 0:
            print(f"      Campaign progress: {i + 1}/{len(campaign_ids)}")

        try:
            # Get URLs clicked in this campaign
            urls_data = _make_request(f"/reports/{campaign_id}/click-details")
            urls = urls_data.get("urls_clicked", [])

            for url in urls:
                if url.get("total_clicks", 0) == 0:
                    continue

                url_id = url.get("id")

                # Fetch all clickers for this URL with pagination
                offset = 0
                count = 100

                while True:
                    members_data = _make_request(
                        f"/reports/{campaign_id}/click-details/{url_id}/members",
                        params={"count": count, "offset": offset}
                    )

                    members = members_data.get("members", [])
                    if not members:
                        break

                    for m in members:
                        # Each member may have multiple clicks (activity array)
                        # But the members endpoint only gives us click count, not timestamps
                        # Generate a single record per subscriber-URL combination
                        all_clicks.append({
                            "campaign_id": campaign_id,
                            "subscriber_hash": m.get("email_id"),
                            "email_address": m.get("email_address"),
                            "url_id": url_id,
                            "url": url.get("url"),
                            "click_count": m.get("clicks", 1),
                        })

                    offset += count

        except Exception as e:
            logger.warning(f"Failed to fetch clicks for campaign {campaign_id}: {e}")

    logger.info(f"Fetched {len(all_clicks)} click records")
    print(f"   Fetched {len(all_clicks)} click records")
    return all_clicks


# =============================================================================
# OUTPUT WRITING
# =============================================================================

def write_entity_jsonl(data: list[dict], entity_name: str, outpath: str = MAILCHIMP_DATA_DIR) -> str:
    """Write entity data to JSONL file.

    Args:
        data: List of dicts to write
        entity_name: Entity name (campaigns, campaign_reports, etc.)
        outpath: Base output directory

    Returns: Path to written file
    """
    if not data:
        logger.warning(f"No data to write for {entity_name}")
        return ""

    # Create directory structure: data/mailchimp/{entity}/
    entity_dir = Path(outpath) / entity_name
    entity_dir.mkdir(parents=True, exist_ok=True)

    # Filename: YYYY-MM-DD.jsonl
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filepath = entity_dir / f"{date_str}.jsonl"

    with open(filepath, "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")

    logger.info(f"Wrote {len(data)} records to {filepath}")
    print(f"   Wrote {len(data)} records to {filepath}")
    return str(filepath)


# =============================================================================
# S3 OPERATIONS
# =============================================================================

def push_to_s3(data_dir: str = MAILCHIMP_DATA_DIR, dev_mode: bool = False) -> None:
    """Upload all JSONL files to S3.

    Args:
        data_dir: Local data directory
        dev_mode: If True, copy to local dev folder instead
    """
    import shutil

    bucket = os.getenv("AWS_BUCKET_NAME")
    base_path = Path(data_dir)

    if not base_path.exists():
        logger.warning(f"Data directory {data_dir} does not exist")
        return

    # Find all JSONL files
    jsonl_files = list(base_path.rglob("*.jsonl"))
    logger.info(f"Found {len(jsonl_files)} JSONL files to upload")

    if dev_mode:
        # Dev mode: copy to local folder
        dev_path = Path(DEV_S3_DIR) / S3_PREFIX
        dev_path.mkdir(parents=True, exist_ok=True)
        print(f"   Copying {len(jsonl_files)} files to dev S3 folder...")

        for f in jsonl_files:
            # Preserve directory structure
            rel_path = f.relative_to(base_path)
            dest = dev_path / rel_path
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(f, dest)
            logger.info(f"Copied {f} to {dest}")

        return

    # Production mode: upload to AWS S3
    print(f"   Uploading {len(jsonl_files)} files to S3 bucket {bucket}...")

    for f in jsonl_files:
        rel_path = f.relative_to(base_path)
        s3_key = f"{S3_PREFIX}/{rel_path}"

        try:
            s3_client.upload_file(str(f), bucket, s3_key)
            logger.info(f"Uploaded {f} to s3://{bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Error uploading {f}: {e}")
            raise


def cleanup_local_files(data_dir: str = MAILCHIMP_DATA_DIR) -> None:
    """Delete local JSONL files after successful S3 upload."""
    base_path = Path(data_dir)

    if not base_path.exists():
        return

    jsonl_files = list(base_path.rglob("*.jsonl"))
    deleted = 0

    for f in jsonl_files:
        try:
            f.unlink()
            deleted += 1
        except Exception as e:
            logger.warning(f"Failed to delete {f}: {e}")

    # Remove empty directories
    for entity_dir in base_path.iterdir():
        if entity_dir.is_dir() and not any(entity_dir.iterdir()):
            entity_dir.rmdir()

    logger.info(f"Cleaned up {deleted} local files")
    print(f"   Cleaned up {deleted} local files")


# =============================================================================
# WORKFLOW ORCHESTRATION
# =============================================================================

def fetch_workflow(lookback_days: int = DEFAULT_CAMPAIGN_LOOKBACK_DAYS) -> None:
    """Main fetch workflow: extract all Mailchimp data.

    Steps:
    1. Fetch campaigns from lookback period
    2. Fetch reports for those campaigns
    3. Fetch subscribers from the campaign lists
    4. Fetch click events for those campaigns
    5. Write all data to JSONL files
    """
    logger.info("=== Starting Mailchimp FETCH workflow ===")
    print("\n=== MAILCHIMP FETCH WORKFLOW ===")

    # 1. Fetch campaigns
    print(f"\n1. Fetching campaigns...")
    campaigns = fetch_campaigns(lookback_days=lookback_days)

    if not campaigns:
        print("\nNo campaigns found. Nothing to fetch.")
        logger.info("=== FETCH workflow completed (no campaigns) ===")
        return

    campaign_ids = [c["campaign_id"] for c in campaigns]
    list_ids = set(c["list_id"] for c in campaigns if c["list_id"])

    # 2. Fetch campaign reports
    print(f"\n2. Fetching campaign reports...")
    reports = fetch_campaign_reports(campaign_ids)

    # 3. Fetch subscribers for all lists
    print(f"\n3. Fetching subscribers...")
    all_subscribers = []
    for list_id in list_ids:
        subscribers = fetch_subscribers(list_id)
        all_subscribers.extend(subscribers)

    # 4. Fetch click events
    print(f"\n4. Fetching click events...")
    clicks = fetch_email_clicks(campaign_ids)

    # 5. Write to JSONL files
    print(f"\n5. Writing data to JSONL files...")
    write_entity_jsonl(campaigns, "campaigns")
    write_entity_jsonl(reports, "campaign_reports")
    write_entity_jsonl(all_subscribers, "email_subscribers")
    write_entity_jsonl(clicks, "email_clicks")

    print(f"\n=== FETCH workflow completed successfully! ===")
    logger.info("=== Mailchimp FETCH workflow completed ===")


def sync_workflow(dev_mode: bool = False) -> None:
    """Sync workflow: upload local files to S3 and cleanup.

    Args:
        dev_mode: If True, copy to local dev folder
    """
    logger.info("=== Starting Mailchimp SYNC workflow ===")
    print("\n=== MAILCHIMP SYNC WORKFLOW ===")

    # 1. Push to S3
    print("\n1. Uploading to S3...")
    push_to_s3(dev_mode=dev_mode)

    # 2. Cleanup local files
    print("\n2. Cleaning up local files...")
    cleanup_local_files()

    print(f"\n=== SYNC workflow completed successfully! ===")
    logger.info("=== Mailchimp SYNC workflow completed ===")


def complete_workflow(lookback_days: int = DEFAULT_CAMPAIGN_LOOKBACK_DAYS, dev_mode: bool = False) -> None:
    """Complete workflow: fetch + sync."""
    logger.info("=== Starting Mailchimp COMPLETE workflow ===")
    print("\n=== MAILCHIMP COMPLETE WORKFLOW (FETCH + SYNC) ===")

    fetch_workflow(lookback_days=lookback_days)
    sync_workflow(dev_mode=dev_mode)

    print(f"\n=== COMPLETE workflow finished! ===")
    logger.info("=== Mailchimp COMPLETE workflow finished ===")

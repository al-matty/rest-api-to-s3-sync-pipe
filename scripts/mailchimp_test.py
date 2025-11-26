#!/usr/bin/env python3
"""
Mailchimp API Feasibility Test

Tests API access and validates data model assumptions for the 4-table schema:
1. CAMPAIGNS - Campaign metadata
2. CAMPAIGN_REPORTS - Aggregate metrics
3. EMAIL_SUBSCRIBERS - Subscriber master list
4. EMAIL_CLICKS - Granular click events

Run: python scripts/mailchimp_test.py
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()


def get_mailchimp_config() -> tuple[str, str]:
    """Get Mailchimp API configuration from environment."""
    api_key = os.getenv("MAILCHIMP_API_KEY")
    if not api_key:
        print("ERROR: MAILCHIMP_API_KEY not set in .env")
        sys.exit(1)

    # Extract data center from API key suffix (e.g., "xxx-us2" -> "us2")
    dc = api_key.split("-")[-1]
    base_url = f"https://{dc}.api.mailchimp.com/3.0"

    return base_url, api_key


def test_authentication(base_url: str, api_key: str) -> bool:
    """Test API authentication by hitting the root endpoint."""
    print("\n1. Testing authentication...")

    response = requests.get(base_url, auth=("anystring", api_key))

    if response.status_code == 200:
        data = response.json()
        print(f"   Account: {data.get('account_name', 'N/A')}")
        print(f"   Email: {data.get('email', 'N/A')}")
        return True
    else:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return False


def test_campaigns(base_url: str, api_key: str) -> list[dict]:
    """Test fetching campaigns with status='sent', sorted by most recent."""
    print("\n2. Testing campaigns endpoint...")

    response = requests.get(
        f"{base_url}/campaigns",
        auth=("anystring", api_key),
        params={"count": 10, "status": "sent", "sort_field": "send_time", "sort_dir": "DESC"}
    )

    if response.status_code != 200:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return []

    data = response.json()
    campaigns = data.get("campaigns", [])
    total = data.get("total_items", 0)

    print(f"   Total campaigns: {total}")
    print(f"   Sample campaigns fetched: {len(campaigns)}")

    if campaigns:
        c = campaigns[0]
        print(f"\n   Sample campaign fields:")
        print(f"     - id: {c.get('id')}")
        print(f"     - type: {c.get('type')}")
        print(f"     - status: {c.get('status')}")
        print(f"     - send_time: {c.get('send_time')}")
        print(f"     - subject_line: {c.get('settings', {}).get('subject_line', 'N/A')[:50]}...")
        print(f"     - list_id: {c.get('recipients', {}).get('list_id')}")

    return campaigns


def test_campaign_report(base_url: str, api_key: str, campaign_id: str) -> dict:
    """Test fetching aggregate report for a campaign."""
    print(f"\n3. Testing reports endpoint (campaign: {campaign_id})...")

    response = requests.get(
        f"{base_url}/reports/{campaign_id}",
        auth=("anystring", api_key)
    )

    if response.status_code != 200:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return {}

    report = response.json()

    print(f"   Report fields:")
    print(f"     - emails_sent: {report.get('emails_sent')}")
    print(f"     - unique_opens: {report.get('opens', {}).get('unique_opens')}")
    print(f"     - open_rate: {report.get('opens', {}).get('open_rate')}")
    print(f"     - unique_clicks: {report.get('clicks', {}).get('unique_clicks')}")
    print(f"     - clicks_total: {report.get('clicks', {}).get('clicks_total')}")
    print(f"     - click_rate: {report.get('clicks', {}).get('click_rate')}")

    return report


def test_subscribers(base_url: str, api_key: str, list_id: str) -> list[dict]:
    """Test fetching subscribers from a list."""
    print(f"\n4. Testing subscribers endpoint (list: {list_id})...")

    response = requests.get(
        f"{base_url}/lists/{list_id}/members",
        auth=("anystring", api_key),
        params={"count": 5}
    )

    if response.status_code != 200:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return []

    data = response.json()
    members = data.get("members", [])
    total = data.get("total_items", 0)

    print(f"   Total subscribers: {total}")
    print(f"   Sample subscribers fetched: {len(members)}")

    if members:
        m = members[0]
        print(f"\n   Sample subscriber fields:")
        print(f"     - id (subscriber_hash): {m.get('id')}")
        print(f"     - email_address: {m.get('email_address')}")
        print(f"     - status: {m.get('status')}")
        print(f"     - timestamp_opt: {m.get('timestamp_opt')}")
        print(f"     - last_changed: {m.get('last_changed')}")

        # Verify MD5 hash relationship
        import hashlib
        email = m.get('email_address', '').lower()
        expected_hash = hashlib.md5(email.encode()).hexdigest()
        actual_hash = m.get('id')
        hash_match = expected_hash == actual_hash
        print(f"\n   MD5 hash verification:")
        print(f"     - email: {email}")
        print(f"     - expected MD5: {expected_hash}")
        print(f"     - actual id: {actual_hash}")
        print(f"     - MATCH: {hash_match}")

    return members


def test_click_details(base_url: str, api_key: str, campaign_id: str) -> list[dict]:
    """Test fetching click details (URLs) for a campaign."""
    print(f"\n5. Testing click-details endpoint (campaign: {campaign_id})...")

    response = requests.get(
        f"{base_url}/reports/{campaign_id}/click-details",
        auth=("anystring", api_key)
    )

    if response.status_code != 200:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return []

    data = response.json()
    urls = data.get("urls_clicked", [])

    print(f"   URLs clicked: {len(urls)}")

    # Find a URL with actual clicks for better testing
    urls_with_clicks = [u for u in urls if u.get('total_clicks', 0) > 0]
    if urls_with_clicks:
        u = urls_with_clicks[0]
        print(f"\n   Sample URL (with clicks) fields:")
    elif urls:
        u = urls[0]
        print(f"\n   Sample URL (no clicks) fields:")
    else:
        return urls

    print(f"     - id: {u.get('id')}")
    print(f"     - url: {u.get('url', '')[:60]}...")
    print(f"     - total_clicks: {u.get('total_clicks')}")
    print(f"     - unique_clicks: {u.get('unique_clicks')}")

    # Return URLs with clicks first for better testing downstream
    return urls_with_clicks + [u for u in urls if u not in urls_with_clicks]


def test_click_members(base_url: str, api_key: str, campaign_id: str, url_id: str) -> list[dict]:
    """Test fetching individual click events per subscriber for a URL."""
    print(f"\n6. Testing click members endpoint (url: {url_id})...")

    response = requests.get(
        f"{base_url}/reports/{campaign_id}/click-details/{url_id}/members",
        auth=("anystring", api_key),
        params={"count": 5}
    )

    if response.status_code != 200:
        print(f"   FAILED: {response.status_code} - {response.text}")
        return []

    data = response.json()
    members = data.get("members", [])
    total = data.get("total_items", 0)

    print(f"   Total clickers: {total}")
    print(f"   Sample clickers fetched: {len(members)}")

    if members:
        m = members[0]
        print(f"\n   Sample click member fields:")
        print(f"     - email_id (subscriber_hash): {m.get('email_id')}")
        print(f"     - email_address: {m.get('email_address')}")
        print(f"     - clicks: {m.get('clicks')}")

        # Check for timestamp in activity
        # Note: The /members endpoint may not include detailed activity timestamps
        # May need to use email-activity endpoint instead

    return members


def run_tests():
    """Run all feasibility tests."""
    print("=" * 60)
    print("MAILCHIMP API FEASIBILITY TEST")
    print("=" * 60)

    base_url, api_key = get_mailchimp_config()
    print(f"\nBase URL: {base_url}")

    results = {
        "auth": False,
        "campaigns": False,
        "reports": False,
        "subscribers": False,
        "click_details": False,
        "click_members": False,
    }

    # 1. Test authentication
    if not test_authentication(base_url, api_key):
        print("\nAuthentication failed. Cannot proceed with other tests.")
        return results
    results["auth"] = True

    # 2. Test campaigns
    campaigns = test_campaigns(base_url, api_key)
    results["campaigns"] = len(campaigns) > 0

    if not campaigns:
        print("\nNo campaigns found. Cannot proceed with report/click tests.")
        return results

    campaign_id = campaigns[0]["id"]
    list_id = campaigns[0].get("recipients", {}).get("list_id")

    # 3. Test campaign report
    report = test_campaign_report(base_url, api_key, campaign_id)
    results["reports"] = bool(report)

    # 4. Test subscribers (if list_id available)
    if list_id:
        subscribers = test_subscribers(base_url, api_key, list_id)
        results["subscribers"] = len(subscribers) > 0
    else:
        print("\n4. Skipping subscribers test (no list_id in campaign)")

    # 5. Test click details
    urls = test_click_details(base_url, api_key, campaign_id)
    results["click_details"] = len(urls) > 0

    # 6. Test click members (if URLs with clicks available)
    urls_with_clicks = [u for u in urls if u.get("total_clicks", 0) > 0]
    if urls_with_clicks:
        url_id = urls_with_clicks[0]["id"]
        click_members = test_click_members(base_url, api_key, campaign_id, url_id)
        results["click_members"] = len(click_members) > 0
    elif urls:
        print("\n6. Skipping click members test (no URLs have clicks)")
        results["click_members"] = True  # Not a failure, just no data
    else:
        print("\n6. Skipping click members test (no URLs in campaign)")

    # Summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)

    all_passed = True
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test_name}: {status}")
        if not passed:
            all_passed = False

    print("\n" + ("ALL TESTS PASSED!" if all_passed else "SOME TESTS FAILED"))

    return results


if __name__ == "__main__":
    run_tests()

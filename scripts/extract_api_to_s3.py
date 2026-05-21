import json
import time
import re
import requests
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("NREL_API_KEY")
BASE_URL = os.getenv(
    "NREL_BASE_URL",
    "https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
)

BUCKET = os.getenv("S3_BUCKET")
PREFIX = os.getenv("RAW_PREFIX", "raw/ev_stations").strip("/")
LIMIT = int(os.getenv("NREL_PAGE_LIMIT", "200"))
REQUEST_TIMEOUT = int(os.getenv("NREL_REQUEST_TIMEOUT_SECONDS", "30"))

s3 = boto3.client("s3")


def get_last_page_from_s3(bucket, prefix):
    """Find highest existing page_NNNNN.json in S3 to resume from."""
    paginator = s3.get_paginator("list_objects_v2")
    max_page = -1

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = re.search(r"page_(\d{5})\.json$", key)
            if m:
                max_page = max(max_page, int(m.group(1)))

    return max_page


def fetch_with_backoff(url, params, max_retries=6):
    for attempt in range(max_retries):
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

        if response.status_code in [429, 502, 503, 504]:
            wait_time = 2 ** attempt
            print(
                f"{response.status_code} error at offset {params.get('offset')}. "
                f"Sleeping {wait_time}s..."
            )
            time.sleep(wait_time)
            continue

        if response.status_code == 410:
            print(
                f"410 Gone at offset {params.get('offset')}. "
                "End of available pagination."
            )
            return None

        response.raise_for_status()
        return response.json()

    print(
        f"Max retries exceeded at offset {params.get('offset')}. "
        "Stopping extraction safely."
    )
    return None


def main():
    if not API_KEY:
        raise ValueError("NREL_API_KEY environment variable is missing")

    if not BUCKET:
        raise ValueError("S3_BUCKET environment variable is missing")

    last_page = get_last_page_from_s3(BUCKET, PREFIX)
    start_page = last_page + 1 if last_page >= 0 else 0
    offset = start_page * LIMIT
    page = start_page

    print(f"Resuming from page {page} (offset {offset})")

    total_new = 0
    total_results = None

    while True:
        params = {
            "fuel_type": "ELEC",
            "limit": LIMIT,
            "offset": offset,
            "api_key": API_KEY,
        }

        data = fetch_with_backoff(BASE_URL, params)

        if data is None:
            print("Stopping extraction safely due to API limit/end of pagination.")
            break

        stations = data.get("fuel_stations", [])

        if total_results is None:
            total_results = data.get("metadata", {}).get("total_results")

        if not stations:
            print("No more stations returned. Done.")
            break

        key = f"{PREFIX}/page_{page:05d}.json"

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(stations),
            ContentType="application/json",
        )

        print(f"Uploaded {len(stations)} -> s3://{BUCKET}/{key}")

        total_new += len(stations)
        page += 1
        offset += LIMIT

        if total_results and offset >= total_results:
            print("Reached total_results from metadata. Done.")
            break

        time.sleep(1)

    print(f"\nDone. Uploaded {total_new} new records starting from page {start_page}.")


if __name__ == "__main__":
    main()

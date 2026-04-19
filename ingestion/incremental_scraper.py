import json
import os
from collections import defaultdict
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account
from google_play_scraper import Sort, reviews
from dotenv import load_dotenv

load_dotenv()

def get_gcs_client(credentials_path: str, project_id: str):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return storage.Client(project=project_id, credentials=credentials)


def get_watermark(client, bucket_name: str) -> datetime:
    """Read last scraped timestamp from watermark file in GCS.
    Falls back to latest review timestamp found in raw bucket partitions."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("watermark.json")

    if blob.exists():
        data = json.loads(blob.download_as_text())
        watermark = datetime.strptime(data["last_scraped_at"], "%Y-%m-%d %H:%M:%S")
        print(f"Watermark found: {watermark}")
        return watermark

    # no watermark file — scan raw bucket for latest review timestamp
    print("No watermark found — scanning raw bucket for latest review timestamp...")

    latest_ts = None
    blobs = client.list_blobs(bucket_name, prefix="raw/")

    for raw_blob in blobs:
        if not raw_blob.name.endswith(".json"):
            continue

        print(f"  Scanning {raw_blob.name}...")
        data = json.loads(raw_blob.download_as_text())

        for review in data:
            dt = review.get("at")
            if dt is None:
                continue
            if not isinstance(dt, datetime):
                dt = datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S")
            if latest_ts is None or dt > latest_ts:
                latest_ts = dt

    if latest_ts:
        print(f"Latest timestamp found in bucket: {latest_ts}")
        # save it as watermark so we don't need to scan again
        save_watermark(client, bucket_name, latest_ts)
        return latest_ts

    # truly empty bucket — start from beginning
    print("No reviews found in bucket — starting from scratch")
    return datetime(2000, 1, 1, 0, 0, 0)


def save_watermark(client, bucket_name: str, last_scraped_at: datetime) -> None:
    """Save latest scraped timestamp to GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("watermark.json")
    blob.upload_from_string(
        json.dumps({"last_scraped_at": str(last_scraped_at)}, indent=2),
        content_type="application/json",
    )
    print(f"Watermark saved: {last_scraped_at}")


def get_month_key(dt) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y/%m")
    return datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S").strftime("%Y/%m")


def upload_partition(
    client,
    bucket_name: str,
    partition_key: str,
    data: list,
) -> None:
    bucket = client.bucket(bucket_name)
    year_month = partition_key.replace("/", "-")
    destination = f"raw/{partition_key}/gcash_reviews_{year_month}.json"
    blob = bucket.blob(destination)

    if blob.exists():
        existing = json.loads(blob.download_as_text())
        existing_ids = {r["reviewId"] for r in existing}
        new_reviews = [r for r in data if r["reviewId"] not in existing_ids]
        data = existing + new_reviews

    blob.upload_from_string(
        json.dumps(data, indent=2, default=str),
        content_type="application/json",
    )
    print(
        f"  Uploaded {len(data):>5} reviews "
        f"→ gs://{bucket_name}/{destination}"
    )


def scrape_incremental(
    bucket_name: str,
    project_id: str,
    credentials_path: str,
    app_id: str = "com.globe.gcash.android",
) -> None:
    client = get_gcs_client(credentials_path, project_id)
    watermark = get_watermark(client, bucket_name)

    continuation_token = None
    total_scraped = 0
    stop_scraping = False
    current_month = None
    current_batch = []
    latest_reviewed_at = watermark

    print(f"Starting incremental scrape from {watermark}...\n")

    while not stop_scraping:
        result, continuation_token = reviews(
            app_id,
            lang="en",
            country="ph",
            sort=Sort.NEWEST,
            count=200,
            continuation_token=continuation_token,
        )

        if not result:
            break

        for review in result:
            dt = review.get("at")
            if not isinstance(dt, datetime):
                dt = datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S")

            # stop once we hit reviews older than or equal to watermark
            if dt <= watermark:
                stop_scraping = True
                break

            month_key = get_month_key(dt)

            # track the latest timestamp seen
            if dt > latest_reviewed_at:
                latest_reviewed_at = dt

            # first review sets current month
            if current_month is None:
                current_month = month_key

            # month boundary crossed — upload completed month
            if month_key != current_month:
                print(f"Month complete: {current_month} ({len(current_batch)} reviews)")
                upload_partition(client, bucket_name, current_month, current_batch)
                current_month = month_key
                current_batch = []

            current_batch.append(review)
            total_scraped += 1

        print(f"Scraped {total_scraped} new reviews so far...")

        if continuation_token is None:
            break

    # upload final batch
    if current_batch:
        print(f"Month complete: {current_month} ({len(current_batch)} reviews)")
        upload_partition(client, bucket_name, current_month, current_batch)

    # save new watermark only if we scraped something
    if total_scraped > 0:
        save_watermark(client, bucket_name, latest_reviewed_at)

    print(f"\nDone! Total new reviews scraped: {total_scraped}")


if __name__ == "__main__":
    project_id       = os.environ["GCP_PROJECT_ID"]
    bucket_name      = os.environ["GCP_RAW_BUCKET"]
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    scrape_incremental(
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_path=credentials_path,
    )
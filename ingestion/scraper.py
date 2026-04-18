import json
import os
from collections import defaultdict
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account
from google_play_scraper import Sort, reviews


def get_gcs_client(credentials_path: str, project_id: str):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return storage.Client(project=project_id, credentials=credentials)


def upload_partition(
    client,
    bucket_name: str,
    partition_key: str,
    data: list,
) -> None:
    bucket = client.bucket(bucket_name)
    
    # partition_key is "2026/04" — convert to "2026-04" for filename
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


def get_month_key(dt) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%Y/%m")
    return datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S").strftime("%Y/%m")


def scrape_and_upload(
    bucket_name: str,
    project_id: str,
    credentials_path: str,
    app_id: str = "com.globe.gcash.android",
) -> None:
    client = get_gcs_client(credentials_path, project_id)
    continuation_token = None
    total_scraped = 0

    # buffer holds reviews for the current month being accumulated
    current_month = None
    current_batch = []

    print("Starting full scrape of GCash reviews...")
    print("Uploading to GCS after each month is fully scraped.\n")

    while True:
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
            if dt is None:
                continue

            month_key = get_month_key(dt)

            # first review sets the current month
            if current_month is None:
                current_month = month_key

            # month boundary crossed — upload the completed month
            if month_key != current_month:
                print(f"Month complete: {current_month} ({len(current_batch)} reviews)")
                upload_partition(client, bucket_name, current_month, current_batch)
                current_month = month_key
                current_batch = []

            current_batch.append(review)

        total_scraped += len(result)
        print(f"Scraped {total_scraped} total | current month: {current_month} ({len(current_batch)} so far)...")

        if continuation_token is None:
            break

    # upload the final month
    if current_batch:
        print(f"Month complete: {current_month} ({len(current_batch)} reviews)")
        upload_partition(client, bucket_name, current_month, current_batch)

    print(f"\nDone! Total reviews scraped: {total_scraped}")


if __name__ == "__main__":
    project_id       = os.environ["GCP_PROJECT_ID"]
    bucket_name      = os.environ["GCP_RAW_BUCKET"]
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    scrape_and_upload(
        bucket_name=bucket_name,
        project_id=project_id,
        credentials_path=credentials_path,
    )
import json
import os
from datetime import datetime

from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

from processing.cleaner import clean_review, assign_sentiment
from processing.categorizer import categorize_review

load_dotenv()

def get_gcs_client(credentials_path: str, project_id: str):
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return storage.Client(project=project_id, credentials=credentials)


def list_raw_partitions(client, raw_bucket: str) -> list:
    """List all year/month partitions in the raw bucket."""
    bucket = client.bucket(raw_bucket)
    blobs = client.list_blobs(raw_bucket, prefix="raw/")
    partitions = []
    for blob in blobs:
        if blob.name.endswith(".json"):
            partitions.append(blob.name)
    return sorted(partitions)


def process_partition(
    client,
    raw_bucket: str,
    processed_bucket: str,
    blob_path: str,
) -> None:
    # derive partition key from path e.g. raw/2026/04/gcash_reviews_2026-04.json
    parts = blob_path.split("/")
    year, month = parts[1], parts[2]
    partition_key = f"{year}/{month}"
    year_month = f"{year}-{month}"
    destination = f"processed/{partition_key}/gcash_reviews_{year_month}.json"

    # skip if already processed
    proc_bucket = client.bucket(processed_bucket)
    dest_blob = proc_bucket.blob(destination)
    if dest_blob.exists():
        print(f"  Skipping {partition_key} — already processed")
        return

    # read raw data
    raw_blob = client.bucket(raw_bucket).blob(blob_path)
    raw_data = json.loads(raw_blob.download_as_text())

    # process each review
    processed = []
    for review in raw_data:
        cleaned = clean_review(review)
        cleaned["sentiment"] = assign_sentiment(cleaned["score"])
        cleaned["category"] = categorize_review(cleaned["content"])
        processed.append(cleaned)

    # upload to processed bucket
    dest_blob.upload_from_string(
        "\n".join(json.dumps(r, default=str) for r in processed),
        content_type="application/json",
    )

    print(
        f"  Processed {len(processed):>5} reviews "
        f"→ gs://{processed_bucket}/{destination}"
    )


def run_pipeline(
    raw_bucket: str,
    processed_bucket: str,
    project_id: str,
    credentials_path: str,
) -> None:
    client = get_gcs_client(credentials_path, project_id)

    print("Listing raw partitions...")
    partitions = list_raw_partitions(client, raw_bucket)
    print(f"Found {len(partitions)} partitions\n")

    for blob_path in partitions:
        print(f"Processing {blob_path}...")
        process_partition(client, raw_bucket, processed_bucket, blob_path)

    print("\nPipeline complete!")


if __name__ == "__main__":
    project_id       = os.environ["GCP_PROJECT_ID"]
    raw_bucket       = os.environ["GCP_RAW_BUCKET"]
    processed_bucket = os.environ["GCP_PROCESSED_BUCKET"]
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    run_pipeline(
        raw_bucket=raw_bucket,
        processed_bucket=processed_bucket,
        project_id=project_id,
        credentials_path=credentials_path,
    )
import json
import os
from datetime import datetime

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

from cleaner import clean_review, assign_sentiment
from categorizer import categorize_review

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

def parse_raw_file(text: str) -> list:
    """Handle both JSON array and newline-delimited JSON formats."""
    text = text.strip()
    if text.startswith("["):
        return json.loads(text)
    else:
        return [
            json.loads(line)
            for line in text.splitlines()
            if line.strip()
        ]

def process_partition(
    client,
    raw_bucket: str,
    processed_bucket: str,
    blob_path: str,
) -> None:
    parts = blob_path.split("/")
    year, month = parts[1], parts[2]
    partition_key = f"{year}/{month}"
    year_month = f"{year}-{month}"
    destination = f"processed/{partition_key}/gcash_reviews_{year_month}.json"

    raw_blob = client.bucket(raw_bucket).blob(blob_path)
    proc_bucket = client.bucket(processed_bucket)
    dest_blob = proc_bucket.blob(destination)

    raw_text = raw_blob.download_as_text()
    raw_data = parse_raw_file(raw_text)

    if dest_blob.exists():
        proc_text = dest_blob.download_as_text()
        proc_count = sum(1 for line in proc_text.splitlines() if line.strip())

        if len(raw_data) == proc_count:
            print(f"  Skipping {partition_key} — already up to date ({proc_count} reviews)")
            return
        else:
            print(f"  Reprocessing {partition_key} — raw has {len(raw_data)}, processed has {proc_count}")

    processed = []
    for review in raw_data:
        cleaned = clean_review(review)
        cleaned["sentiment"] = assign_sentiment(cleaned["score"])
        cleaned["category"] = categorize_review(cleaned["content"])
        processed.append(cleaned)

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
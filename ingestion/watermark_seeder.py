import json
from google.cloud import storage
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    r'C:/Users/kimma/Documents/Projects/GCash Reviews Pipeline/keys/gcash-reviews-pipeline-29e00e6a272b.json'
)

client = storage.Client(project='gcash-reviews-pipeline', credentials=credentials)
bucket = client.bucket('gcash-reviews-raw')

blobs = client.list_blobs(bucket)

latest_blob = None
latest_time = None

for blob in blobs:
    if blob.time_created is None:
        continue

    if latest_time is None or blob.time_created > latest_time:
        latest_time = blob.time_created
        latest_blob = blob

if latest_time:
    watermark = {
        "last_scraped_at": latest_time.strftime("%Y-%m-%d %H:%M:%S"),
        "source_blob": latest_blob.name
    }
else:
    watermark = {
        "last_scraped_at": None,
        "source_blob": None
    }

blob = bucket.blob('watermark.json')
blob.upload_from_string(
    json.dumps(watermark, indent=2),
    content_type='application/json'
)

print("Watermark updated:", watermark)
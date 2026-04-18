output "raw_bucket_name" {
  value = google_storage_bucket.raw.name
}

output "processed_bucket_name" {
  value = google_storage_bucket.processed.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.gcash_reviews.dataset_id
}
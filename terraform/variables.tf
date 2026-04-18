variable "credentials" {
  description = "My Credentials"
  default     = "../keys/gcash-reviews-pipeline-29e00e6a272b.json"
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-southeast1"
}

variable "raw_bucket_name" {
  description = "GCS raw bucket name"
  type        = string
}

variable "processed_bucket_name" {
  description = "GCS processed bucket name"
  type        = string
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "gcash_reviews"
}
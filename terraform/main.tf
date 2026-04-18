terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials)
}

# GCS Raw Bucket (Bronze)
resource "google_storage_bucket" "raw" {
  name          = var.raw_bucket_name
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = "true"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# GCS Processed Bucket (Silver)
resource "google_storage_bucket" "processed" {
  name          = var.processed_bucket_name
  location      = var.region
  force_destroy = true
  uniform_bucket_level_access = "true"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset (Gold)
resource "google_bigquery_dataset" "gcash_reviews" {
  dataset_id  = var.bq_dataset_id
  location    = var.region
  description = "GCash reviews analytics dataset"

  delete_contents_on_destroy = true
}
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.18.0"
    }
  }
}

provider "google" {
  credentials = "./keys/my-creads.json"
  project     = "studious-vector-447814-r7"
  region      = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "studious-vector-447814-r7-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id                  = "demo_dataset"
}

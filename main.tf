# 1. Google Provider Configuration
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("key.json")
  project     = "shriom-de-2026"
  region      = "asia-south1"
}

# 2. Data Lake: Google Cloud Storage Bucket
resource "google_storage_bucket" "taxi_lake" {
  name          = "shriom-ny-taxi-lake-2026" # Bucket name globally unique hona chahiye
  location      = "asia-south1"
  force_destroy = true # Testing ke time poora bucket ek command se delete ho sake

  lifecycle_rule {
    condition {
      age = 30 # 30 din baad temporary files auto delete
    }
    action {
      type = "Delete"
    }
  }
}

# 3. Data Warehouse: BigQuery Dataset
resource "google_bigquery_dataset" "taxi_dataset" {
  dataset_id  = "ny_taxi_data"
  friendly_name = "NYC Taxi Trip Analytics"
  description = "Bronze, Silver aur Gold tables ke liye dataset"
  location    = "asia-south1"
}

# 4. Outputs (Create hone ke baad screen pe dikhenge)
output "gcs_bucket_name" {
  value = google_storage_bucket.taxi_lake.name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.taxi_dataset.dataset_id
}
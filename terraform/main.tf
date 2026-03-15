provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket for data lake
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

# GCS Bucket for DBT models
resource "google_storage_bucket" "dbt_models" {
  name          = "${var.project_id}-dbt-models"
  location      = var.region
  force_destroy = true
}

# Service Account for the pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "data-pipeline-sa"
  display_name = "Data Pipeline Service Account"
}

# IAM permissions
resource "google_storage_bucket_iam_member" "data_lake_writer" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Dataproc cluster for Spark (optional - if using GCP managed Spark)
resource "google_dataproc_cluster" "spark_cluster" {
  name   = "spark-kafka-cluster"
  region = var.region
  
  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }
    
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }
    
    software_config {
      image_version = "2.0-debian10"
      optional_components = ["KAFKA", "ZOOKEEPER"]
    }
  }
}
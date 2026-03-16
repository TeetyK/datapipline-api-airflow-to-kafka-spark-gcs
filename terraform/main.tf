#Terraform Google provider
provider "google"{
    project = var.project_id
    region = var.region
    # user_project_override = true
    # billing_project = var.billing_id
    zone = var.zone
    credentials = file(var.service_account_path)
}

locals {
    sa_key = jsondecode(file(var.service_account_path))
    sa_email = local.sa_key.client_email
}

#Terraform Google stronge bucket
resource "google_storage_bucket" "gcs_bucket_api"{
    name = var.gcs_bucket_name
    location = var.location
    force_destroy = true

    # requester_pays = false
    # uniform_bucket_level_access = true

    # versioning {
    #     enabled =false
    # }
}

# resource "google_storage_bucket_iam_member" "bucket_owner"{
#     bucket = google_storage_bucket.gcs_bucket.name
#     role = "roles/storage.admin"
#     member = "serviceAccount:${local.sa_email}"
# }

# bronze layer
resource "google_bigquery_dataset" "bronze" {
    dataset_id = var.bigquery_bronze_dataset_name
    description = "This's a bronze layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000
    
    # access {
    #     role    ="WRITER"
    #     user_by_email = local.sa_email
    # }
}
# silver layer
resource "google_bigquery_dataset" "silver" {
    dataset_id = var.bigquery_silver_dataset_name
    description = "This's a silver layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000

    #  access {
    #     role    ="WRITER"
    #     user_by_email = local.sa_email
    # }
}
# gold layer
resource "google_bigquery_dataset" "gold" {
    dataset_id = var.bigquery_gold_dataset_name
    description = "This's a gold layer in BugQuery"
    location = var.location
    default_table_expiration_ms = 3600000

    #  access {
    #     role    ="WRITER"
    #     user_by_email = local.sa_email
    # }
}

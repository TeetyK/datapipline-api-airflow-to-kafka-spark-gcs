variable "service_account_path" {
  description = "Google Cloud Service Account"
  default     = "F:\\datapipline-api-airflow-to-kafka-spark-gcs\\keys\\api-datapipeline-2-ef3107a0390f.json"
}
variable "project_id" {
  description = "Google Project ID"
  default     = "api-datapipeline-2"
}
variable "billing_id" {
  description = "Google Billing Project"
  default     = "api-datapipeline-2"
}
variable "api_gcs_bucket_name" {
  description = "GCS Bucket Name"
  default     = "gcs_bucket_api_1"
}
variable "region" {
  description = "Google Cloud region"
  default     = "us-central1"
}
variable "zone" {
  description = "Google Cloud zone"
  default     = "us-central1-a"
}
variable "location" {
  description = "Google Cloud location"
  default     = "US"
}
variable "bigquery_bronze_dataset_name" {
  description = "Bigquery bronze layer"
  default     = "bronze"
}
variable "bigquery_silver_dataset_name" {
  description = "Bigquery silver layer"
  default     = "silver"
}
variable "bigquery_gold_dataset_name" {
  description = "Bigquery gold layer"
  default     = "gold"
}



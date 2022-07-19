locals {
  data_lake_bucket = "data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  type = string
  default = "cpereira-teste"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type = string
  default = "europe-west2"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  type = string
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "coin_price"
}

variable "db_prod_dataset" {
  description = "BigQuery Dataset for storing production data after Dataproc transformation"
  type = string
  default = "coin_price_production"
}
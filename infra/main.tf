resource "google_project_service" "apis" {
  for_each = toset([
    "cloudresourcemanager.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "artifactregistry.googleapis.com",
    "aiplatform.googleapis.com",
    "run.googleapis.com",
    "iam.googleapis.com"
  ])

  service = each.key
  disable_on_destroy = false
}

resource "google_storage_bucket" "data_bucket" {
  project       = var.project_id
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  versioning {
    enabled = true
  }
  depends_on = [google_project_service.apis]
}

resource "google_bigquery_dataset" "bq_projeto" {
  project     = var.project_id
  dataset_id  = "bq_ml"
  location    = var.region
  depends_on = [google_project_service.apis]
}

resource "google_artifact_registry_repository" "api_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = "api-repo"
  format        = "DOCKER"

  depends_on = [google_project_service.apis]
}
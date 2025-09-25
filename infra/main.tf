# Enables all necessary APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com", 
    "storage.googleapis.com", 
    "bigquery.googleapis.com",
    "iam.googleapis.com", 
    "artifactregistry.googleapis.com", 
    "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com", 
    "cloudfunctions.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

# 1. Artifact Registry
resource "google_artifact_registry_repository" "api_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = "api-repo"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# 2. Cloud Storage Bucket
resource "google_storage_bucket" "data_bucket" {
  project       = var.project_id
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  versioning { enabled = true }
  depends_on = [google_project_service.apis]
}

# 3. BigQuery Dataset
resource "google_bigquery_dataset" "bq_projeto" {
  project    = var.project_id
  dataset_id = "bq_ml"
  location   = var.region
  depends_on = [google_project_service.apis]
}

# 4. BigQuery External Table
resource "google_bigquery_table" "data_table" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_projeto.dataset_id
  table_id   = "reservatorios"

  
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_bucket.name}/ano=*"]
    hive_partitioning_options {
      mode              = "AUTO"
      source_uri_prefix = "gs://${google_storage_bucket.data_bucket.name}"
    }
  }
}

# 5. IAM: Service Accounts
resource "google_service_account" "api_sa" {
  project      = var.project_id
  account_id   = "ons-api-runner-sa"
  display_name = "ONS API Service Account"
}
resource "google_service_account" "cf_sa" {
  project      = var.project_id
  account_id   = "ingest-trigger-cf-sa"
  display_name = "Cloud Function Trigger SA"
}
resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "scheduler-runner-sa"
  display_name = "Scheduler Runner SA"
}

# 6. IAM: Permissions
resource "google_storage_bucket_iam_member" "api_storage_writer" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.api_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "api_bigquery_reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.bq_projeto.dataset_id
  role       = "roles/bigquery.dataViewer" 
  member     = "serviceAccount:${google_service_account.api_sa.email}"
}

resource "google_cloud_run_service_iam_member" "cf_invoker" {
  service  = google_cloud_run_v2_service.api_service.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.cf_sa.email}"
}

resource "google_cloudfunctions2_function_iam_member" "scheduler_invoker" {
  project        = google_cloudfunctions2_function.scheduler_function.project
  location       = google_cloudfunctions2_function.scheduler_function.location
  cloud_function = google_cloudfunctions2_function.scheduler_function.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_service_account.scheduler_sa.email}"
}

# 7. Cloud Function
resource "google_cloudfunctions2_function" "scheduler_function" {
  project  = var.project_id
  name     = "daily-ingest-trigger"
  location = var.region
  build_config {
    runtime     = "python310"
    entry_point = "trigger_ingest_pipeline"
    source {
      storage_source {
        bucket = google_storage_bucket.data_bucket.name
        object = "source/scheduler_function.zip"
      }
    }
  }
  service_config {
    service_account_email = google_service_account.cf_sa.email
    environment_variables = {
      INGEST_API_URL = "${google_cloud_run_v2_service.api_service.uri}/ingest"
    }
  }
}

# 8. Cloud Run Service
resource "google_cloud_run_v2_service" "api_service" {
  project  = var.project_id
  name     = "ons-data-api"
  location = var.region
  deletion_protection = false
  
  template {
    service_account = google_service_account.api_sa.email

    containers {
      image = var.docker_image_url

      # Formato correto: um bloco 'env' para cada variável
      env {
        name  = "BUCKET_NAME"
        value = google_storage_bucket.data_bucket.name
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BIGQUERY_DATASET"
        value = google_bigquery_dataset.bq_projeto.dataset_id
      }
      env {
        name  = "BIGQUERY_TABLE"
        value = google_bigquery_table.data_table.table_id
      }
    }
  }
}

# Allows public access to the API
resource "google_cloud_run_service_iam_member" "public_access" {
  service  = google_cloud_run_v2_service.api_service.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# 9. Cloud Scheduler
resource "google_cloud_scheduler_job" "ingest_job" {
  project   = var.project_id
  name      = "daily-ons-ingest"
  schedule  = "0 3 * * *"
  time_zone = "America/Sao_Paulo"
  http_target {
    uri = google_cloudfunctions2_function.scheduler_function.service_config[0].uri
    oidc_token {
      service_account_email = google_service_account.scheduler_sa.email
    }
  }
}
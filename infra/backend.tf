terraform {
  backend "gcs" {
    bucket  = "terraform-state-rowadi"
    prefix  = "terraform/state"
  }
}
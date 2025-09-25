terraform {
  backend "gcs" {
    bucket  = "terraform-state-rowadi"
    prefix  = "terraform/state"
  }
}

variable "project_id" {
  type        = string
}

variable "region" {
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  type        = string
}

variable "docker_image_url" {
  type        = string
}
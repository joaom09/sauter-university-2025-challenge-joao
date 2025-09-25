variable "project_id" {
  description = "O ID do seu projeto no Google Cloud."
  type        = string
}

variable "region" {
  description = "A região onde os recursos serão criados."
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "O nome do Cloud Storage Bucket. DEVE SER ÚNICO GLOBALMENTE."
  type        = string
}

variable "docker_image_url" {
  description = "A URL completa da imagem Docker a ser usada pelo Cloud Run."
  type        = string
}
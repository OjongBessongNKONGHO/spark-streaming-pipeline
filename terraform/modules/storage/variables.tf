# ─────────────────────────────────────────────────────────────
# Storage Module Variables
# ─────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Name of the project — used to tag all resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "bucket_name" {
  description = "Name of the S3 bucket for Delta Lake storage — must be globally unique"
  type        = string
}
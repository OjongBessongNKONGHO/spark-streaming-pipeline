# ─────────────────────────────────────────────────────────────
# Compute Module Variables
# ─────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Name of the project — used to tag all resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type — t3.medium minimum for Spark (needs 4GB RAM)"
  type        = string
  default     = "t3.medium"
}

variable "subnet_id" {
  description = "ID of the public subnet to deploy EC2 into"
  type        = string
}

variable "security_group_id" {
  description = "ID of the EC2 security group"
  type        = string
}

variable "key_pair_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket used for Delta Lake storage"
  type        = string
}

variable "aws_region" {
  description = "AWS region for S3 access"
  type        = string
  default     = "eu-west-3"
}
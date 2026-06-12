# ─────────────────────────────────────────────────────────────
# Root Module Variables
# spark-streaming-pipeline
# ─────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Name of the project — used as prefix for all resources"
  type        = string
  default     = "spark-streaming"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-west-3"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "Availability zones in Paris region"
  type        = list(string)
  default     = ["eu-west-3a", "eu-west-3b"]
}

variable "instance_type" {
  description = "EC2 instance type — t3.medium minimum for Spark"
  type        = string
  default     = "t3.medium"
}

variable "key_pair_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
}

variable "bucket_name" {
  description = "Name of the S3 bucket for Delta Lake — must be globally unique"
  type        = string
}

variable "alarm_email" {
  description = "Email address for CloudWatch alarm notifications"
  type        = string
}
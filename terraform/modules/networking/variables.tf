# ─────────────────────────────────────────────────────────────
# Networking Module Variables
# ─────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Name of the project — used to tag all resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets — EC2 instance lives here"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24"]
}

variable "availability_zones" {
  description = "Availability zones in Paris region"
  type        = list(string)
  default     = ["eu-west-3a", "eu-west-3b"]
}
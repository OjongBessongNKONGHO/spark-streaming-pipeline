# ─────────────────────────────────────────────────────────────
# Root Module — Spark Structured Streaming Pipeline
# Provisions networking, compute and storage for the pipeline
# Region: eu-west-3 (Paris)
# ─────────────────────────────────────────────────────────────

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─────────────────────────────────────────────────────────────
# Networking — VPC, subnets, internet gateway, security groups
# ─────────────────────────────────────────────────────────────
module "networking" {
  source = "./modules/networking"

  project_name        = var.project_name
  environment         = var.environment
  vpc_cidr            = var.vpc_cidr
  public_subnet_cidrs = var.public_subnet_cidrs
  availability_zones  = var.availability_zones
}

# ─────────────────────────────────────────────────────────────
# Storage — S3 bucket for Delta Lake
# Created before compute so the bucket ARN is available for IAM
# ─────────────────────────────────────────────────────────────
module "storage" {
  source = "./modules/storage"

  project_name = var.project_name
  environment  = var.environment
  bucket_name  = var.bucket_name
}

# ─────────────────────────────────────────────────────────────
# Compute — EC2 instance with Spark, Docker and Java 17
# Depends on networking and storage modules
# ─────────────────────────────────────────────────────────────
module "compute" {
  source = "./modules/compute"

  project_name      = var.project_name
  environment       = var.environment
  instance_type     = var.instance_type
  subnet_id         = module.networking.public_subnet_ids[0]
  security_group_id = module.networking.ec2_security_group_id
  key_pair_name     = var.key_pair_name
  s3_bucket_arn     = module.storage.bucket_arn
  aws_region        = var.aws_region
  alarm_email       = var.alarm_email

  depends_on = [module.networking, module.storage]
}
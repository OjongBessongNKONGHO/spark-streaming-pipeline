# ─────────────────────────────────────────────────────────────
# Storage Module — S3 Delta Lake for Spark Streaming Pipeline
# spark-streaming-pipeline
# Cost estimate: $0.023/GB/month (Standard storage, eu-west-3)
# ─────────────────────────────────────────────────────────────

# S3 bucket — stores Delta Lake tables, checkpoints and analytics
# Cost estimate: $0.023/GB/month Standard
# Standard-IA after 30 days: $0.0125/GB/month (saves ~46%)
resource "aws_s3_bucket" "delta_lake" {
  bucket = var.bucket_name

  tags = {
    Name        = var.bucket_name
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Purpose     = "Delta Lake - Spark Structured Streaming"
  }
}

# Block all public access — Delta Lake data is private
resource "aws_s3_bucket_public_access_block" "delta_lake" {
  bucket                  = aws_s3_bucket.delta_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable versioning — keeps history of Delta Lake transaction log files
# Delta Lake relies on the _delta_log for ACID transactions
resource "aws_s3_bucket_versioning" "delta_lake" {
  bucket = aws_s3_bucket.delta_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption — all Delta Lake data encrypted at rest
resource "aws_s3_bucket_server_side_encryption_configuration" "delta_lake" {
  bucket = aws_s3_bucket.delta_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle policy — move older Delta Lake data to cheaper storage
# Raw weather partitions older than 30 days move to Standard-IA
# Checkpoints are small and accessed frequently — keep in Standard
resource "aws_s3_bucket_lifecycle_configuration" "delta_lake" {
  bucket = aws_s3_bucket.delta_lake.id

  # Raw Delta Lake weather data — move to IA after 30 days
  rule {
    id     = "delta-weather-lifecycle"
    status = "Enabled"

    filter {
      prefix = "delta/weather/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }

  # Analytics results — move to IA after 30 days
  rule {
    id     = "delta-analytics-lifecycle"
    status = "Enabled"

    filter {
      prefix = "delta/analytics/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
  }
}

# Delta Lake folder structure
# delta/weather/     — raw Spark Structured Streaming output
# delta/analytics/   — batch analysis results
# checkpoints/       — Spark streaming checkpoints for fault tolerance
# logs/              — pipeline logs

resource "aws_s3_object" "delta_weather" {
  bucket  = aws_s3_bucket.delta_lake.id
  key     = "delta/weather/"
  content = ""
}

resource "aws_s3_object" "delta_analytics" {
  bucket  = aws_s3_bucket.delta_lake.id
  key     = "delta/analytics/"
  content = ""
}

resource "aws_s3_object" "checkpoints" {
  bucket  = aws_s3_bucket.delta_lake.id
  key     = "checkpoints/"
  content = ""
}

resource "aws_s3_object" "logs" {
  bucket  = aws_s3_bucket.delta_lake.id
  key     = "logs/"
  content = ""
}
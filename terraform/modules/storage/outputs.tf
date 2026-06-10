# ─────────────────────────────────────────────────────────────
# Storage Module Outputs
# ─────────────────────────────────────────────────────────────

output "bucket_id" {
  description = "ID of the S3 Delta Lake bucket"
  value       = aws_s3_bucket.delta_lake.id
}

output "bucket_arn" {
  description = "ARN of the S3 Delta Lake bucket — used by EC2 IAM policy"
  value       = aws_s3_bucket.delta_lake.arn
}

output "bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = aws_s3_bucket.delta_lake.bucket_domain_name
}

output "delta_weather_path" {
  description = "S3 path for Delta Lake weather data"
  value       = "s3a://${aws_s3_bucket.delta_lake.id}/delta/weather"
}

output "delta_analytics_path" {
  description = "S3 path for Delta Lake analytics results"
  value       = "s3a://${aws_s3_bucket.delta_lake.id}/delta/analytics"
}

output "checkpoint_path" {
  description = "S3 path for Spark streaming checkpoints"
  value       = "s3a://${aws_s3_bucket.delta_lake.id}/checkpoints"
}
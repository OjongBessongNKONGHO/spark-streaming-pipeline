# ─────────────────────────────────────────────────────────────
# Root Module Outputs
# spark-streaming-pipeline
# ─────────────────────────────────────────────────────────────

output "vpc_id" {
  description = "ID of the VPC"
  value       = module.networking.vpc_id
}

output "public_subnet_ids" {
  description = "IDs of the public subnets"
  value       = module.networking.public_subnet_ids
}

output "ec2_instance_id" {
  description = "ID of the EC2 instance"
  value       = module.compute.instance_id
}

output "ec2_public_ip" {
  description = "Public IP of the EC2 instance — use this to SSH and access Kafka UI"
  value       = module.compute.public_ip
}

output "ec2_private_ip" {
  description = "Private IP of the EC2 instance"
  value       = module.compute.private_ip
}

output "s3_bucket_id" {
  description = "ID of the Delta Lake S3 bucket"
  value       = module.storage.bucket_id
}

output "delta_weather_path" {
  description = "S3 path for Delta Lake weather data — use in DELTA_LAKE_PATH env var"
  value       = module.storage.delta_weather_path
}

output "checkpoint_path" {
  description = "S3 path for Spark checkpoints — use in CHECKPOINT_PATH env var"
  value       = module.storage.checkpoint_path
}

output "ssh_command" {
  description = "SSH command to connect to the EC2 instance"
  value       = "ssh -i ${var.key_pair_name}.pem ec2-user@${module.compute.public_ip}"
}
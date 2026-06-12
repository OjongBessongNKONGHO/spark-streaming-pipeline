# ─────────────────────────────────────────────────────────────
# Compute Module Outputs
# ─────────────────────────────────────────────────────────────

output "instance_id" {
  description = "ID of the EC2 instance"
  value       = aws_instance.pipeline.id
}

output "public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_eip.pipeline.public_ip
}

output "private_ip" {
  description = "Private IP address of the EC2 instance"
  value       = aws_instance.pipeline.private_ip
}

output "instance_arn" {
  description = "ARN of the EC2 instance"
  value       = aws_instance.pipeline.arn
}

output "iam_role_arn" {
  description = "ARN of the IAM role attached to the EC2 instance"
  value       = aws_iam_role.ec2_role.arn
}
output "alarm_topic_arn" {
  description = "ARN of the SNS topic for CloudWatch alarms"
  value       = aws_sns_topic.alarms.arn
}
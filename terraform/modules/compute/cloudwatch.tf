# ─────────────────────────────────────────────────────────────
# CloudWatch Monitoring — EC2 Alarms
# spark-streaming-pipeline
# Alerts via SNS email when the instance is unhealthy or overloaded
# ─────────────────────────────────────────────────────────────

# SNS topic — alarm notifications are published here
resource "aws_sns_topic" "alarms" {
  name = "${var.project_name}-alarms"

  tags = {
    Name        = "${var.project_name}-alarms"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Email subscription — requires manual confirmation via email link
resource "aws_sns_topic_subscription" "alarm_email" {
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# Alarm — CPU utilization sustained above 80%
# Catches runaway Spark jobs or undersized instance type
resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  alarm_name          = "${var.project_name}-high-cpu"
  alarm_description   = "EC2 CPU utilization above 80% for 10 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 80

  dimensions = {
    InstanceId = aws_instance.pipeline.id
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = {
    Name        = "${var.project_name}-high-cpu-alarm"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# Alarm — instance status check failed
# Catches hardware/hypervisor issues requiring a stop/start to resolve
resource "aws_cloudwatch_metric_alarm" "status_check_failed" {
  alarm_name          = "${var.project_name}-status-check-failed"
  alarm_description   = "EC2 instance status check failed — instance may need a restart"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "StatusCheckFailed_Instance"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Maximum"
  threshold           = 0

  dimensions = {
    InstanceId = aws_instance.pipeline.id
  }

  alarm_actions = [aws_sns_topic.alarms.arn]
  ok_actions    = [aws_sns_topic.alarms.arn]

  tags = {
    Name        = "${var.project_name}-status-check-alarm"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
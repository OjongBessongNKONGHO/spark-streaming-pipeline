# ─────────────────────────────────────────────────────────────
# Compute Module — EC2 Instance for Spark Streaming Pipeline
# spark-streaming-pipeline
# Cost estimate: t3.medium = $0.0416/hour = ~$30/month (eu-west-3)
# Spark needs more memory than a simple ETL — t3.medium has 4GB RAM
# ─────────────────────────────────────────────────────────────

# Get the latest Amazon Linux 2023 AMI for eu-west-3
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# IAM Role — allows EC2 to access S3 (Delta Lake) and CloudWatch
resource "aws_iam_role" "ec2_role" {
  name = "${var.project_name}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.project_name}-ec2-role"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# IAM Policy — S3 read/write for Delta Lake storage
# EC2 needs full access to the Delta Lake bucket — read checkpoints,
# write Parquet files, list partitions
resource "aws_iam_role_policy" "ec2_s3_policy" {
  name = "${var.project_name}-ec2-s3-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*"
        ]
      }
    ]
  })
}

# IAM Policy — CloudWatch logs and metrics for Spark monitoring
resource "aws_iam_role_policy" "ec2_cloudwatch_policy" {
  name = "${var.project_name}-ec2-cloudwatch-policy"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData",
          "cloudwatch:GetMetricData",
          "cloudwatch:ListMetrics",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}

# IAM Instance Profile — attaches the role to the EC2 instance
resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# EC2 Instance — runs the full Spark streaming pipeline
# Cost estimate: t3.medium = $0.0416/hour = ~$30/month (eu-west-3)
# t3.medium chosen over t3.micro because Spark requires minimum 2GB RAM
# t3.micro (1GB) causes OutOfMemoryError on Spark startup
resource "aws_instance" "pipeline" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [var.security_group_id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name
  key_name               = var.key_pair_name

  # User data — installs Docker, Java 17, and pipeline dependencies on first boot
  user_data = base64encode(<<-EOF
    #!/bin/bash
    set -e

    # Update system
    yum update -y

    # Install Java 17 — required for Spark 3.5
    yum install -y java-17-amazon-corretto

    # Install Docker
    yum install -y docker
    systemctl start docker
    systemctl enable docker
    usermod -aG docker ec2-user

    # Install Docker Compose
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
      -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose

    # Install Python 3 and pip
    yum install -y python3 python3-pip git

    # Install AWS CLI v2
    pip3 install awscli

    # Install CloudWatch agent
    yum install -y amazon-cloudwatch-agent

    # Set AWS region for S3 access
    echo "export AWS_DEFAULT_REGION=${var.aws_region}" >> /etc/environment

    # Create pipeline directory
    mkdir -p /opt/spark-streaming-pipeline
    chown ec2-user:ec2-user /opt/spark-streaming-pipeline

    # Clone the pipeline repository
    cd /opt/spark-streaming-pipeline
    git clone https://github.com/OjongBessongNKONGHO/spark-streaming-pipeline.git .

    # Log setup completion
    echo "spark-streaming-pipeline EC2 setup complete" >> /var/log/pipeline-setup.log
    echo "Instance: $(hostname)" >> /var/log/pipeline-setup.log
    echo "Date: $(date)" >> /var/log/pipeline-setup.log
    echo "Java version: $(java -version 2>&1)" >> /var/log/pipeline-setup.log
  EOF
  )

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 30
    delete_on_termination = true

    tags = {
      Name        = "${var.project_name}-root-volume"
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }

  }

  # Pin the AMI once deployed — most_recent AMI lookups drift over time
  # as AWS publishes new AL2023 images, which would force an unwanted
  # instance replacement on every `terraform apply`. Ignore AMI changes
  # to keep this instance stable; new deployments still get the latest AMI.
  lifecycle {
    ignore_changes = [ami]
  }

  tags = {
    Name        = "${var.project_name}-spark-ec2"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Role        = "SparkStreamingPipeline"
  }
}

# Elastic IP — gives the EC2 instance a fixed public IP
# Cost estimate: $0.005/hour when associated with running instance
resource "aws_eip" "pipeline" {
  instance = aws_instance.pipeline.id
  domain   = "vpc"

  tags = {
    Name        = "${var.project_name}-eip"
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}
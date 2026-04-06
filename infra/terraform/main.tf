terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {
    bucket = "vkreddy-tf-state"
    key    = "healthcare-claims-etl/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" { region = var.aws_region }

locals {
  tags = { Project = "healthcare-claims-etl", Owner = "vikas-reddy", ManagedBy = "terraform" }
}

# ---------------------------------------------------------------------------
# S3 buckets
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "raw"       { bucket = "vkreddy-claims-raw";     tags = local.tags }
resource "aws_s3_bucket" "lake"      { bucket = "vkreddy-claims-lake";    tags = local.tags }
resource "aws_s3_bucket" "tmp"       { bucket = "vkreddy-claims-tmp";     tags = local.tags }
resource "aws_s3_bucket" "scripts"   { bucket = "vkreddy-claims-scripts"; tags = local.tags }

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

# ---------------------------------------------------------------------------
# Glue jobs
# ---------------------------------------------------------------------------
resource "aws_glue_job" "extract" {
  name         = "extract_claims"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/extract_claims.py"
    python_version  = "3"
  }
  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.tmp.bucket}/spark-ui/"
    "--extra-py-files"                   = "s3://${aws_s3_bucket.scripts.bucket}/libs/utils.zip"
  }
  number_of_workers = 10
  worker_type       = "G.1X"
  tags              = local.tags
}

resource "aws_glue_job" "transform" {
  name         = "transform_claims"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/transform_claims.py"
    python_version  = "3"
  }
  number_of_workers = 20
  worker_type       = "G.2X"
  tags              = local.tags
}

resource "aws_glue_job" "load_redshift" {
  name         = "load_redshift"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.scripts.bucket}/glue_jobs/load_redshift.py"
    python_version  = "3"
  }
  number_of_workers = 10
  worker_type       = "G.1X"
  tags              = local.tags
}

# ---------------------------------------------------------------------------
# Redshift cluster
# ---------------------------------------------------------------------------
resource "aws_redshift_cluster" "claims_dw" {
  cluster_identifier     = "vkreddy-claims-dw"
  database_name          = "claims_dw"
  master_username        = "admin"
  master_password        = var.redshift_password
  node_type              = "ra3.xlplus"
  cluster_type           = "multi-node"
  number_of_nodes        = 2
  vpc_security_group_ids = [aws_security_group.redshift_sg.id]
  skip_final_snapshot    = false
  tags                   = local.tags
}

resource "aws_security_group" "redshift_sg" {
  name   = "redshift-claims-sg"
  vpc_id = var.vpc_id
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = local.tags
}

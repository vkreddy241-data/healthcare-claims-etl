variable "aws_region"         { default = "us-east-1" }
variable "vpc_id"             { type = string }
variable "redshift_password"  { type = string; sensitive = true }

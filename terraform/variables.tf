variable "aws_region" {
  default     = "eu-west-2"
  type        = string
  description = "AWS region to deploy resources"
}

variable "aws_profile" {
  default = "dev"
}

variable "bucket_name" {
  default     = "dublin-trips-data-lake"
  type        = string
  description = "S3 bucket name for raw data"
}

variable "redshift_namespace" {
  default = "dublin-trips-namespace"
}

variable "redshift_workgroup" {
  default     = "dublin-trips-workgroup"
  type        = string
  description = "Redshift Serverless workgroup name"
}

variable "redshift_admin_user" {
  type        = string
  description = "Admin username for Redshift"
}

variable "redshift_admin_password" {
  type        = string
  description = "Admin password for Redshift"
}
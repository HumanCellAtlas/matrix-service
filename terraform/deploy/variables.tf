variable "aws_profile" {}

variable "aws_region" {}

variable "hca_ms_merged_mtx_bucket" {
  description = "The name of s3 bucket which stores merged matrices from matrix service"
}

variable "hca_ms_request_bucket" {
  description = "The name of s3 bucket which stores request status file from matrix service"
}

variable "hca_ms_deployment_bucket" {
  description = "The name of s3 bucket which stores lambda deployment package"
}

variable "ms_sqs_queue" {
  description = "The name of SQS queue for the matrix service"
}

variable "ms_secret_name" {
  description = "The name of the secret in aws secret manager"
}

variable "app_version" {}
variable "aws_profile" {}

variable "aws_region" {
  default = "us-east-1"
}

variable "hca_ms_merged_mtx_bucket" {
  default = "hca-ms-merged-mtx"
  description = "The name of s3 bucket which stores merged matrices from matrix service"
}

variable "hca_ms_request_bucket" {
  default = "hca-ms-request-status"
  description = "The name of s3 bucket which stores request status file from matrix service"
}

variable "hca_ms_deployment_bucket" {
  default = "hca-ms-deployment"
  description = "The name of s3 bucket which stores lambda deployment package"
}

variable "ms_sqs_queue" {
  default = "hca-ms-queue"
  description = "The name of SQS queue for the matrix service"
}

variable "ms_secret_name" {
  default = "matrix-service/dev/secrets"
}
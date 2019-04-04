variable "deployment_stage" {
  type = "string"
}

variable "account_id" {
  type = "string"
}

variable "aws_region" {
  type = "string"
}

variable "converter_cluster_ami_id" {
  type = "string"
}

variable "vpc_cidr_block" {
  type = "string"
}

variable "redshift_username" {
  type = "string"
}

variable "redshift_password" {
  type = "string"
}

variable "gcp_service_acct_creds" {
  type = "string"
}

variable "query_runner_concurrency" {
  type = "string"
  default = "1"
}

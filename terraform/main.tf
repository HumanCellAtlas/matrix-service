provider "aws" {
  region  = "${var.region}"
  profile = "${var.profile}"
}

data "aws_caller_identity" "aws_caller" {}

terraform {
  backend "s3" {}
}

data "terraform_remote_state" "matrix_service_backend-state" {
  backend = "s3"
  config {
    bucket  = "${var.bucket}"
    key     = "${var.key}"
    region  = "${var.region}"
    profile = "${var.profile}"
  }
}
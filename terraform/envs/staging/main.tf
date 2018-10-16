terraform {
  required_version = "=0.11.8"

  backend "s3" {
    bucket  = "org-humancellatlas-861229788715-terraform"
    key     = "matrix-service/envs/staging/terraform.tfvars"
    encrypt = true
    region  = "us-east-1"
    profile = "hca"
  }
}

provider "aws" {
  version = ">= 1.38"
  region = "${var.aws_region}"
  profile = "hca"
}

module "matrix_service_infra" {
  source = "../../modules/matrix-service/infra"
  deployment_stage = "${var.deployment_stage}"
  account_id = "${var.account_id}"
  aws_region = "${var.aws_region}"
}

module "matrix_service_lambdas" {
  source = "../../modules/matrix-service/lambdas"
  deployment_stage = "${var.deployment_stage}"
  account_id = "${var.account_id}"
  aws_region = "${var.aws_region}"
  deployment_bucket_id = "${module.matrix_service_infra.deployment_bucket_id}"
}

terraform {
  required_version = "=0.11.10"

  backend "s3" {
    bucket  = "org-humancellatlas-861229788715-terraform"
    key     = "matrix-service/envs/dev/terraform.tfstate"
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
  converter_cluster_ami_id = "${var.converter_cluster_ami_id}"
  vpc_cidr_block = "${var.vpc_cidr_block}"
  gcp_service_acct_creds = "${var.gcp_service_acct_creds}"
  query_runner_concurrency = "${var.query_runner_concurrency}"
}

module "matrix_service_lambdas" {
  source = "../../modules/matrix-service/lambdas"
  deployment_stage = "${var.deployment_stage}"
  account_id = "${var.account_id}"
  aws_region = "${var.aws_region}"
  deployment_bucket_id = "${module.matrix_service_infra.deployment_bucket_id}"
  results_bucket_arn = "${module.matrix_service_infra.results_bucket_arn}"
}

module "matrix_service_redshift" {
  source = "../../modules/matrix-service/redshift"
  deployment_stage = "${var.deployment_stage}"
  redshift_username = "${var.redshift_username}"
  redshift_password = "${var.redshift_password}"
  vpc_id = "${module.matrix_service_infra.vpc_id}"
  vpc_subnet_ids = "${module.matrix_service_infra.vpc_subnet_ids}"
}

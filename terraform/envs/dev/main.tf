terraform {
  required_version = "=0.11.8"

  backend "s3" {
    bucket  = "org-humancellatlas-861229788715-terraform"
    key     = "matrix-service/envs/dev/terraform.tfvars"
    encrypt = true
    region  = "us-east-1"
    profile = "hca"
  }
}

provider "aws" {
  version = ">= 1.38"
  region = "us-east-1"
  profile = "hca"
}


module "matrix-service" {
  source = "../../modules/matrix-service"
  deployment_stage = "${var.deployment_stage}"
}

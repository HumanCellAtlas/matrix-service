resource "aws_s3_bucket" "hca_ms_merged_mtx_bucket" {
  bucket = "${var.hca_ms_merged_mtx_bucket}",
  acl    = "public-read"
}

resource "aws_s3_bucket" "hca_ms_request_bucket" {
  bucket = "${var.hca_ms_request_bucket}",
  acl    = "private"
}

resource "aws_s3_bucket" "hca_ms_deployment_bucket" {
  bucket = "${var.hca_ms_deployment_bucket}",
  acl    = "private"
}
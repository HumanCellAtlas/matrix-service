resource "aws_s3_bucket" "hca_ms_merged_mtx_bucket" {
  bucket        = "${var.hca_ms_merged_mtx_bucket}",
  acl           = "public-read"
  force_destroy = true
}
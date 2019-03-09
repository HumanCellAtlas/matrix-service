resource "aws_s3_bucket" "matrix_service_preload" {
  bucket = "dcp-matrix-service-preload-${var.deployment_stage}"

  lifecycle_rule {
    id      = "matrix_service_preload_expiration"

    expiration {
      days = 7
    }

    enabled = true
  }
}

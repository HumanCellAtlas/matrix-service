resource "aws_s3_bucket" "matrix-results" {
  bucket = "dcp-matrix-service-results-${var.deployment_stage}"
  acl    = "public-read"

  lifecycle_rule {
    id      = "matrix_service_results_expiration"

    expiration {
      days = 28
    }

    enabled = true
  }
  # Add tags later
}

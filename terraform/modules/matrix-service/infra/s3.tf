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

resource "aws_s3_bucket" "matrix_service_lambda_deployment_bucket" {
    bucket = "dcp-matrix-service-lambda-deployment-${var.deployment_stage}"
    acl = "private"
    force_destroy = "false"
    acceleration_status = "Enabled"
}

output "deployment_bucket_id" {
  value = "${aws_s3_bucket.matrix_service_lambda_deployment_bucket.id}"
}

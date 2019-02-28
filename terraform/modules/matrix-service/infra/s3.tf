resource "aws_s3_bucket" "matrix-results" {
  bucket = "dcp-matrix-service-results-${var.deployment_stage}"
  acl    = "public-read"
}

resource "aws_s3_bucket_policy" "matrix_results_bucket_policy" {
  bucket = "${aws_s3_bucket.matrix-results.id}"
  policy =<<POLICY
{
  "Version": "2012-10-17",
  "Id": "AddPublicRead",
  "Statement": [
    {
      "Sid": "AllowPublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:ListBucket", "s3:GetObject"],
      "Resource": ["${aws_s3_bucket.matrix-results.arn}", "${aws_s3_bucket.matrix-results.arn}/*"]
    }
  ]
}
POLICY
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

output "results_bucket_arn" {
  value = "${aws_s3_bucket.matrix-results.arn}"
}

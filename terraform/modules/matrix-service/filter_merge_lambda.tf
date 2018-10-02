resource "aws_iam_role" "matrix_service_filter_merge_lambda" {
  name = "matrix-service-filter-merge-daemon-${var.deployment_stage}"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "matrix_service_filter_merge_lambda"{
  name = "matrix-service-filter-merge-daemon-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_filter_merge_lambda.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LambdaPolicy",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*",
      "Effect": "Allow"
    }
  ]
}
EOF
}

resource "aws_s3_bucket" "lambda_area_bucket" {
    bucket = "dcp-matrix-service-filter-merge-lambda-deployment-${var.deployment_stage}"
    acl = "private"
    force_destroy = "false"
    acceleration_status = "Enabled"
}

resource "aws_lambda_function" "matrix_service_filter_merge_lambda" {
  function_name    = "dcp-matrix-service-driver-${var.deployment_stage}"
  s3_bucket        = "${aws_s3_bucket.lambda_area_bucket.id}"
  s3_key           = "filter_merge_daemon.zip"
  role             = "${aws_iam_role.matrix_service_filter_merge_lambda.arn}"
  handler          = "app.driver_handler"
  runtime          = "python3.6"
  timeout          = 300

  environment {
    variables = {
        DEPLOYMENT_STAGE = "${var.deployment_stage}"
    }
  }
}
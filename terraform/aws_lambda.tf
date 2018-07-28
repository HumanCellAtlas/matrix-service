resource "aws_lambda_function" "matrix_service_api" {
  function_name = "matrix-service-api"
  s3_bucket     = "${var.hca_ms_deployment_bucket}"
  s3_key        = "v${var.app_version}/deployment.zip"
  description   = "Matrix Service API Lambda Function"
  handler       = "app.app"
  role          = "${aws_iam_role.matrix_service_lambda_role.arn}"
  timeout       = 300
  runtime       = "python3.6"
}

resource "aws_lambda_function" "matrix_service_sqs_handler" {
  function_name = "matrix-service-sqs-listener"
  s3_bucket     = "${var.hca_ms_deployment_bucket}"
  s3_key        = "v${var.app_version}/deployment.zip"
  description   = "Matrix Service SQS Listener Lambda Function"
  handler       = "app.ms_sqs_queue_listener"
  role          = "${aws_iam_role.matrix_service_lambda_role.arn}"
  timeout       = 300
  runtime       = "python3.6"
}

resource "aws_lambda_function" "matrix_service_dlq_handler" {
  function_name = "matrix-service-dlq-listener"
  s3_bucket     = "${var.hca_ms_deployment_bucket}"
  s3_key        = "v${var.app_version}/deployment.zip"
  description   = "Matrix Service DLQ Listener Lambda Function"
  handler       = "app.ms_dead_letter_queue_listener"
  role          = "${aws_iam_role.matrix_service_lambda_role.arn}"
  timeout       = 30
  runtime       = "python3.6"
}


resource "aws_lambda_event_source_mapping" "matrix_service_sqs_lambda_event_source_mapping" {
  event_source_arn  = "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.aws_caller.account_id}:${var.ms_sqs_queue}"
  function_name     = "${aws_lambda_function.matrix_service_sqs_handler.arn}"
  starting_position = ""
}

resource "aws_lambda_event_source_mapping" "matrix_service_dlq_lambda_event_source_mapping" {
  event_source_arn  = "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.aws_caller.account_id}:${var.ms_dead_letter_queue}"
  function_name     = "${aws_lambda_function.matrix_service_dlq_handler.arn}"
  starting_position = ""
}

resource "aws_lambda_permission" "matrix_service_api_gateway_permisson" {
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.matrix_service_api.arn}"
  principal     = "apigateway.amazonaws.com"
}

resource "aws_iam_role" "matrix_service_lambda_role" {
  name               = "matrix_service_lambda_role"
  assume_role_policy = <<EOF
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
EOF
}

resource "aws_iam_role_policy" "matrix_service_policy" {
  name   = "matrix_service_policy"
  role   = "${aws_iam_role.matrix_service_lambda_role.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "s3:ListBucket",
                "s3:DeleteObject",
                "sqs:GetQueueUrl",
                "sqs:SendMessageBatch",
                "sqs:ReceiveMessage",
                "sqs:SendMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            "Resource": [
                "arn:aws:s3:::${var.hca_ms_merged_mtx_bucket}",
                "arn:aws:s3:::${var.hca_ms_request_bucket}",
                "arn:aws:s3:::${var.hca_ms_merged_mtx_bucket}/*",
                "arn:aws:s3:::${var.hca_ms_request_bucket}/*",
                "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.aws_caller.account_id}:secret:${var.ms_secret_name}*",
                "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.aws_caller.account_id}:${var.ms_sqs_queue}",
                "arn:aws:sqs:${var.aws_region}:${data.aws_caller_identity.aws_caller.account_id}:${var.ms_dead_letter_queue}"
            ]
        }
    ]
}
EOF
}

resource "aws_iam_role_policy" "matrix_service_cloudwatch_policy" {
  name   = "matrix_service_cloudwatch_policy"
  role   = "${aws_iam_role.matrix_service_lambda_role.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
EOF
}
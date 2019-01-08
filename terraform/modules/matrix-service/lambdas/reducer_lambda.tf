resource "aws_iam_role" "matrix_service_reducer_lambda" {

  name = "matrix-service-reducer-daemon-${var.deployment_stage}"

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

resource "aws_iam_role_policy" "matrix_service_reducer_lambda" {
  name = "matrix-service-reducer-daemon-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_reducer_lambda.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LogsPolicy",
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-reducer-${var.deployment_stage}",
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-reducer-${var.deployment_stage}:*:*"
      ],
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
    },
    {
      "Sid": "DynamoPolicy",
      "Effect": "Allow",
      "Action": [
        "dynamodb:UpdateItem",
        "dynamodb:GetItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-state-table-${var.deployment_stage}",
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-output-table-${var.deployment_stage}",
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-cache-table-${var.deployment_stage}"
      ]
    },
    {
      "Sid": "S3Policy",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": [
        "${var.results_bucket_arn}",
        "${var.results_bucket_arn}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_lambda_function" "matrix_service_reducer_lambda" {
  function_name    = "dcp-matrix-service-reducer-${var.deployment_stage}"
  s3_bucket        = "${var.deployment_bucket_id}"
  s3_key           = "reducer_daemon.zip"
  role             = "${aws_iam_role.matrix_service_reducer_lambda.arn}"
  handler          = "app.reducer_handler"
  runtime          = "python3.6"
  timeout          = 900

  environment {
    variables = {
      DEPLOYMENT_STAGE = "${var.deployment_stage}"
      DYNAMO_STATE_TABLE_NAME = "dcp-matrix-service-state-table-${var.deployment_stage}"
      DYNAMO_OUTPUT_TABLE_NAME = "dcp-matrix-service-output-table-${var.deployment_stage}"
      DYNAMO_CACHE_TABLE_NAME = "dcp-matrix-service-cache-table-${var.deployment_stage}"
      S3_RESULTS_BUCKET = "dcp-matrix-service-results-${var.deployment_stage}"
      BATCH_CONVERTER_JOB_QUEUE_ARN = "arn:aws:batch:${var.aws_region}:${var.account_id}:job-queue/dcp-matrix-converter-queue-${var.deployment_stage}"
      BATCH_CONVERTER_JOB_DEFINITION_ARN = "arn:aws:batch:${var.aws_region}:${var.account_id}:job-definition/dcp-matrix-converter-job-definition-${var.deployment_stage}"
    }
  }
}

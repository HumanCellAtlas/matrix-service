resource "aws_iam_role" "matrix_service_driver_lambda" {
  name = "matrix-service-driver-daemon-${var.deployment_stage}"

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

resource "aws_iam_role_policy" "matrix_service_driver_lambda" {
  name = "matrix-service-driver-daemon-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_driver_lambda.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LogsPolicy",
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-driver-${var.deployment_stage}",
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-driver-${var.deployment_stage}:*:*"
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
        "dynamodb:GetItem",
        "dynamodb:PutItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-state-table-${var.deployment_stage}",
        "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-output-table-${var.deployment_stage}"
      ]
    },
    {
      "Sid": "LambdaPolicy",
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:${var.aws_region}:${var.account_id}:function:dcp-matrix-service-mapper-${var.deployment_stage}"
    },
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
          "s3:ListAllMyBuckets",
          "s3:HeadBucket"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::dcp-matrix-service-queries-${var.deployment_stage}",
        "arn:aws:s3:::dcp-matrix-service-queries-${var.deployment_stage}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:DescribeSecret",
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:${var.aws_region}:${var.account_id}:secret:dcp/matrix/${var.deployment_stage}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": [
        "arn:aws:sqs:${var.aws_region}:${var.account_id}:dcp-matrix-query-queue-${var.deployment_stage}"
      ]
    }
  ]
}
EOF
}

resource "aws_lambda_function" "matrix_service_driver_lambda" {
  function_name    = "dcp-matrix-service-driver-${var.deployment_stage}"
  s3_bucket        = "${var.deployment_bucket_id}"
  s3_key           = "driver_daemon.zip"
  role             = "${aws_iam_role.matrix_service_driver_lambda.arn}"
  handler          = "app.driver_handler"
  runtime          = "python3.6"
  timeout          = 300

  environment {
    variables = {
        DEPLOYMENT_STAGE = "${var.deployment_stage}"
        DYNAMO_STATE_TABLE_NAME="dcp-matrix-service-state-table-${var.deployment_stage}"
        DYNAMO_OUTPUT_TABLE_NAME="dcp-matrix-service-output-table-${var.deployment_stage}"
        MATRIX_QUERY_BUCKET = "dcp-matrix-service-queries-${var.deployment_stage}"
        MATRIX_RESULTS_BUCKET = "dcp-matrix-service-results-${var.deployment_stage}"
    }
  }
}

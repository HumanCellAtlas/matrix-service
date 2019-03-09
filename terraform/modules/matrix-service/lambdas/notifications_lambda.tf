resource "aws_iam_role" "matrix_service_notifications_lambda" {
  name = "matrix-service-notifications-daemon-${var.deployment_stage}"

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

resource "aws_iam_role_policy" "matrix_service_notifications_lambda" {
  name = "matrix-service-notifications-daemon-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_notifications_lambda.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LogsPolicy",
      "Effect": "Allow",
      "Resource": [
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-notifications-${var.deployment_stage}",
        "arn:aws:logs:${var.aws_region}:${var.account_id}:log-group:/aws/lambda/dcp-matrix-service-notifications-${var.deployment_stage}:*:*"
      ],
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ]
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
        "arn:aws:s3:::dcp-matrix-service-preload-${var.deployment_stage}",
        "arn:aws:s3:::dcp-matrix-service-preload-${var.deployment_stage}/*"
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
    }
  ]
}
EOF
}

resource "aws_lambda_function" "matrix_service_notifications_lambda" {
  function_name    = "dcp-matrix-service-notifications-${var.deployment_stage}"
  s3_bucket        = "${var.deployment_bucket_id}"
  s3_key           = "notifications_daemon.zip"
  role             = "${aws_iam_role.matrix_service_notifications_lambda.arn}"
  handler          = "app.notifications_handler"
  runtime          = "python3.6"
  timeout          = 900
  memory_size      = 3008

  environment {
    variables = {
      DEPLOYMENT_STAGE = "${var.deployment_stage}"
      MATRIX_PRELOAD_BUCKET = "dcp-matrix-service-preload-${var.deployment_stage}"
      MATRIX_REDSHIFT_IAM_ROLE_ARN = "arn:aws:iam::${var.account_id}:role/matrix-service-redshift-${var.deployment_stage}"
      XDG_CONFIG_HOME = "/tmp"
    }
  }
}

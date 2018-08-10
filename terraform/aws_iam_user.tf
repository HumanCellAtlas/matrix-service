# Define policy for IAM user, "matrix-service", which is used to run the unittest
resource "aws_iam_user_policy" "matrix_service_test_policy" {
  name   = "matrix-service-testcase-policy"
  user   = "matrix-service"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sqs:GetQueueUrl",
                "sqs:DeleteMessage",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "sqs:ReceiveMessage",
                "sqs:SendMessage"
            ],
            "Resource": [
                "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.aws_caller.account_id}:secret:${var.ms_secret_name}*",
                "arn:aws:sqs:${var.region}:${data.aws_caller_identity.aws_caller.account_id}:${var.ms_sqs_queue}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": "arn:aws:s3:::${var.hca_ms_merged_mtx_bucket}/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:${var.region}:${data.aws_caller_identity.aws_caller.account_id}:table/${var.ms_dynamodb}",
                "arn:aws:dynamodb:*:*:table/*/index/*"
            ]
        }
    ]
}
EOF
}
provider "aws" {
  region  = "${var.aws_region}"
  profile = "${var.aws_profile}"
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

resource "aws_iam_role_policy" "matrix_service_lambda_role_policy" {
  name   = "matrix_service_lambda_role_policy"
  role   = "${aws_iam_role.matrix_service_lambda_role.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData",
                "s3:ListAllMyBuckets",
                "s3:HeadBucket",
                "logs:CreateLogGroup"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:DeleteMessage",
                "secretsmanager:DescribeSecret",
                "s3:GetObjectVersionTagging",
                "sqs:SendMessageBatch",
                "sqs:ReceiveMessage",
                "s3:GetObjectAcl",
                "s3:GetObjectVersionAcl",
                "s3:PutObjectTagging",
                "s3:DeleteObject",
                "s3:GetIpConfiguration",
                "s3:DeleteObjectTagging",
                "sqs:GetQueueUrl",
                "s3:GetBucketWebsite",
                "sqs:SendMessage",
                "sqs:GetQueueAttributes",
                "s3:DeleteObjectVersionTagging",
                "s3:GetBucketNotification",
                "s3:GetReplicationConfiguration",
                "s3:ListMultipartUploadParts",
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetAnalyticsConfiguration",
                "s3:GetObjectVersionForReplication",
                "s3:ListBucketByTags",
                "s3:GetLifecycleConfiguration",
                "s3:GetBucketTagging",
                "s3:GetInventoryConfiguration",
                "s3:DeleteObjectVersion",
                "s3:ListBucketVersions",
                "s3:GetBucketLogging",
                "s3:ListBucket",
                "s3:GetAccelerateConfiguration",
                "sqs:ListQueueTags",
                "s3:GetBucketPolicy",
                "s3:GetObjectVersionTorrent",
                "s3:GetEncryptionConfiguration",
                "secretsmanager:GetSecretValue",
                "s3:GetBucketRequestPayment",
                "sqs:DeleteMessageBatch",
                "s3:GetObjectTagging",
                "s3:GetMetricsConfiguration",
                "s3:ListBucketMultipartUploads",
                "s3:PutObjectVersionTagging",
                "s3:GetBucketVersioning",
                "s3:GetBucketAcl",
                "logs:PutLogEvents",
                "s3:GetObjectTorrent",
                "sqs:ListDeadLetterSourceQueues",
                "s3:GetBucketCORS",
                "s3:GetBucketLocation",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::hca-ms-merged-mtx",
                "arn:aws:s3:::hca-ms-request-status",
                "arn:aws:s3:::hca-ms-merged-mtx/*",
                "arn:aws:s3:::hca-ms-request-status/*",
                "arn:aws:secretsmanager:us-east-1:861229788715:secret:hca/dcp/matrix-service/secrets-RFZR9D",
                "arn:aws:logs:*:*:log-group:*:*:*",
                "arn:aws:sqs:us-east-1:861229788715:hca-ms-queue"
            ]
        }
    ]
}
EOF
}
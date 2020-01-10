resource "aws_iam_instance_profile" "ec2_loader" {
  name = "matrix-service-redshift-loader-${var.deployment_stage}"
  role =  aws_iam_role.ec2_loader.name
}

resource "aws_iam_role" "ec2_loader" {
  name = "matrix-service-redshift-loader-${var.deployment_stage}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ec2.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "ec2_loader" {
  name = "matrix-service-redshift-loader-policy-${var.deployment_stage}"
  role =  aws_iam_role.ec2_loader.id
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
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
        }
    ]
}
EOF
}

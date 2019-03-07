resource "aws_iam_instance_profile" "matrix_service_etl_ec2" {
  name = "matrix-service-etl-ec2-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_etl_ec2.name}"
}

resource "aws_iam_role" "matrix_service_etl_ec2" {
  name = "matrix-service-etl-ec2-${var.deployment_stage}"

  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy" "matrix_service_etl_ec2" {
  name = "matrix-service-etl-ec2-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_etl_ec2.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "s3",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::dcp-matrix-service-redshift-preload-${var.deployment_stage}",
        "arn:aws:s3:::dcp-matrix-service-redshift-preload-${var.deployment_stage}/*"
      ]
    }
  ]
}
EOF
}

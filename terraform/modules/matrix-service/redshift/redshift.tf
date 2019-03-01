resource "aws_redshift_cluster" "default" {
  cluster_identifier = "dcp-matrix-service-cluster-${var.deployment_stage}"
  database_name      = "matrix_service_${var.deployment_stage}"
  master_username    = "${var.redshift_username}"
  master_password    = "${var.redshift_password}"
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes    = 4
}

resource "aws_security_group" "matrix_service_redshift_sg" {
  name = "dcp-matrix-service-redshift-sg"

  vpc_id = "vpc-3aa13b43"
  ingress {
    from_port = 0
    to_port = 0
    protocol = "-1"
  }
}

resource "aws_iam_role" "matrix_service_redshift" {
  name = "matrix-service-redshift-${var.deployment_stage}"

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

resource "aws_iam_role_policy" "matrix_service_redshift" {
  name = "matrix-service-redshift-${var.deployment_stage}"
  role = "${aws_iam_role.matrix_service_redshift.name}"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "s3",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:PutObject",
        "s3:GetObject"
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

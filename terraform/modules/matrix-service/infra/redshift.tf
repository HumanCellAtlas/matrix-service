resource "aws_redshift_cluster" "default" {
  cluster_identifier = "dcp-matrix-service-cluster-${var.deployment_stage}"
  database_name      = "matrix_service_${var.deployment_stage}"
  master_username    =  var.redshift_username
  master_password    =  var.redshift_password
  node_type          = "dc2.large"
  cluster_type       = "multi-node"
  number_of_nodes    = 4
  iam_roles          = ["${aws_iam_role.matrix_service_redshift.arn}"]
  cluster_subnet_group_name =  aws_redshift_subnet_group.matrix_service_redshift.name
  vpc_security_group_ids = ["${aws_security_group.matrix_service_redshift_sg.id}"]
}

resource "aws_redshift_subnet_group" "matrix_service_redshift" {
  name = "matrix-service-redshift-subnet-${var.deployment_stage}"
  subnet_ids = ["${data.aws_subnet_ids.matrix_vpc.ids}"]
}

resource "aws_security_group" "matrix_service_redshift_sg" {
  vpc_id =  aws_vpc.vpc.id
  name = "matrix-service-rs-sg-${var.deployment_stage}"

  ingress {
    protocol = "tcp"
    self = true
    from_port = 5439
    to_port = 5439
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol = "tcp"
    from_port = 5439
    to_port = 5439
    cidr_blocks = ["0.0.0.0/0"]
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
        "Service": "redshift.amazonaws.com"
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
  role =  aws_iam_role.matrix_service_redshift.name
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::dcp-matrix-service-preload-${var.deployment_stage}",
        "arn:aws:s3:::dcp-matrix-service-preload-${var.deployment_stage}/*",
        "arn:aws:s3:::dcp-matrix-service-query-results-${var.deployment_stage}",
        "arn:aws:s3:::dcp-matrix-service-query-results-${var.deployment_stage}/*"
      ]
    }
  ]
}
EOF
}

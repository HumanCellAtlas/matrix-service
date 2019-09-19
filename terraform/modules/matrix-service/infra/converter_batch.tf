resource "aws_iam_instance_profile" "ecsInstanceRole" {
  name = "ecsInstanceRole"
  role = "${aws_iam_role.ecsInstanceRole.name}"
}

resource "aws_iam_role" "ecsInstanceRole" {
  name = "ecsInstanceRole"
  path = "/"
  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "ecsInstanceRole" {
  role = "${aws_iam_role.ecsInstanceRole.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role" "AWSBatchServiceRole" {
  name = "AWSBatchServiceRole"
  path = "/service-role/"
  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "batch.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "AWSBatchServiceRole" {
  role = "${aws_iam_role.AWSBatchServiceRole.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role" "AmazonEC2SpotFleetRole" {
  name = "AmazonEC2SpotFleetRole"
  path = "/"
  description = "Role to Allow EC2 Spot Fleet to request and terminate Spot Instances on your behalf."
  assume_role_policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "spotfleet.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "AmazonEC2SpotFleetRole" {
  role = "${aws_iam_role.AmazonEC2SpotFleetRole.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetRole"
}

resource aws_iam_service_linked_role "AWSServiceRoleForEC2Spot" {
  aws_service_name = "spot.amazonaws.com"
  description = "Allows EC2 Spot to launch and manage spot instances."
}

resource aws_iam_service_linked_role "AWSServiceRoleForEC2SpotFleet" {
  aws_service_name = "spotfleet.amazonaws.com"
  description = "Default EC2 Spot Fleet Service Linked Role"
}

data "external" "converter_desired_vcpus" {
  program = ["python", "${path.module}/fetch_batch_vcpus.py"]

  query = {
    compute_environment_name = "dcp-matrix-converter-cluster-${var.deployment_stage}"
  }
}

resource "aws_batch_compute_environment" "converter_compute_env" {
  compute_environment_name = "dcp-matrix-converter-cluster-${var.deployment_stage}"
  type = "MANAGED"
  service_role = "${aws_iam_role.AWSBatchServiceRole.arn}"
  compute_resources {
    type = "SPOT"
    bid_percentage = 100
    spot_iam_fleet_role = "${aws_iam_role.AmazonEC2SpotFleetRole.arn}"
    max_vcpus = 256
    min_vcpus = 4
    // You must set desired_vcpus otherwise you get error: "desiredvCpus should be between minvCpus and maxvCpus"
    // However this is actually not settable in AWS.  It will not let you change it.
    // Here we use an external data source to dynamically set the desired vcpus to match current state.
    desired_vcpus = "${data.external.converter_desired_vcpus.result.desired_vcpus}"
    instance_type = [
      "m4"
    ]
    image_id = "${var.converter_cluster_ami_id}"
    subnets = ["${data.aws_subnet_ids.matrix_vpc.ids}"]
    security_group_ids = [
      "${aws_vpc.vpc.default_security_group_id}"
    ]
    ec2_key_pair = "matrix-${var.deployment_stage}"
    instance_role = "${aws_iam_instance_profile.ecsInstanceRole.arn}"
  }
  depends_on = [
    "aws_iam_role_policy_attachment.AWSBatchServiceRole"
  ]
}

resource "aws_batch_job_queue" "converter_job_queue" {
  name = "dcp-matrix-converter-queue-${var.deployment_stage}"
  compute_environments = [
    "${aws_batch_compute_environment.converter_compute_env.arn}"]
  priority = 1
  state = "ENABLED"
}

resource "aws_iam_policy" "converter_job_policy" {
  name = "dcp-matrix-converter-job-${var.deployment_stage}"
  policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "s3",
        "Effect": "Allow",
        "Action": [
          "s3:*"
        ],
        "Resource": [
          "arn:aws:s3:::dcp-matrix-service-results-${var.deployment_stage}",
          "arn:aws:s3:::dcp-matrix-service-results-${var.deployment_stage}/*",
          "arn:aws:s3:::dcp-matrix-service-query-results-${var.deployment_stage}",
          "arn:aws:s3:::dcp-matrix-service-query-results-${var.deployment_stage}/*"
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
          "arn:aws:dynamodb:${var.aws_region}:${var.account_id}:table/dcp-matrix-service-request-table-${var.deployment_stage}"
        ]
      },
      {
        "Sid": "S3ReadFors3fs",
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
          "cloudwatch:PutMetricData"
        ],
        "Resource": "*"
      }
    ]
}
POLICY
}

resource "aws_iam_role" "converter_job_role" {
  name = "dcp-matrix-converter-job-${var.deployment_stage}"
  assume_role_policy = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "ecs-tasks.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
POLICY
}

resource "aws_iam_role_policy_attachment" "converter_job_role" {
  role = "${aws_iam_role.converter_job_role.name}"
  policy_arn = "${aws_iam_policy.converter_job_policy.arn}"
}

resource "aws_batch_job_definition" "converter_job_def" {
    name = "dcp-matrix-converter-job-definition-${var.deployment_stage}"
    type = "container"
    container_properties = <<CONTAINER_PROPERTIES
{
  "command": [],
  "image": "humancellatlas/matrix-converter:32",
  "memory": 8192,
  "vcpus": 2,
  "jobRoleArn": "${aws_iam_role.converter_job_role.arn}",
  "volumes": [{
    "host": {
      "sourcePath": "/data"
    },
    "name": "data"
  }],
  "mountPoints": [{
    "containerPath": "/data",
    "readOnly": false,
    "sourceVolume": "data"
  }]
}
CONTAINER_PROPERTIES
}

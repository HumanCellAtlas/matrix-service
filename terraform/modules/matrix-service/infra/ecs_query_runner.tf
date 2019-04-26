resource "aws_ecs_task_definition" "query_runner" {
  family                = "matrix-service-query-runner-${var.deployment_stage}"
  requires_compatibilities = ["FARGATE"]
  execution_role_arn = "${aws_iam_role.task_executor.arn}"
  task_role_arn = "${aws_iam_role.query_runner.arn}"
  container_definitions = <<DEFINITION
[
  {
    "environment": [
      {
        "name": "DEPLOYMENT_STAGE",
        "value": "${var.deployment_stage}"
      },
      {
        "name": "MATRIX_QUERY_BUCKET",
        "value": "dcp-matrix-service-queries-${var.deployment_stage}"
      },
      {
        "name": "MATRIX_RESULTS_BUCKET",
        "value": "dcp-matrix-service-results-${var.deployment_stage}"
      },
      {
        "name": "DYNAMO_STATE_TABLE_NAME",
        "value": "dcp-matrix-service-state-table-${var.deployment_stage}"
      },
      {
        "name": "DYNAMO_OUTPUT_TABLE_NAME",
        "value": "dcp-matrix-service-output-table-${var.deployment_stage}"
      },
      {
        "name": "BATCH_CONVERTER_JOB_QUEUE_ARN",
        "value": "arn:aws:batch:${var.aws_region}:${var.account_id}:job-queue/dcp-matrix-converter-queue-${var.deployment_stage}"
      },
      {
        "name": "BATCH_CONVERTER_JOB_DEFINITION_ARN",
        "value": "arn:aws:batch:${var.aws_region}:${var.account_id}:job-definition/dcp-matrix-converter-job-definition-${var.deployment_stage}"
      }
    ],
    "ulimits": [
      {
        "softLimit": 4100,
        "hardLimit": 4100,
        "name": "nofile"
      }
    ],
    "memory": 512,
    "cpu": 256,
    "image": "humancellatlas/matrix-query-runner:4",
    "name": "query-runner-${var.deployment_stage}",
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "${aws_cloudwatch_log_group.query_runner.name}",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
    }
  }
]
DEFINITION
  network_mode          = "awsvpc"
  cpu                   = "256"
  memory                = "512"
}

resource "aws_cloudwatch_log_group" "query_runner" {
  name              = "/aws/service/matrix-service-query-runner-${var.deployment_stage}"
  retention_in_days = 1827
}

resource "aws_iam_role" "task_executor" {
  name = "matrix-service-task-execution-role-${var.deployment_stage}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs.amazonaws.com",
          "ecs-tasks.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "task_executor_ecs" {
  role = "${aws_iam_role.task_executor.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "query_runner" {
  name = "matrix-service-query-runner-${var.deployment_stage}"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "ecs-tasks.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "query_runner" {
  name = "matrix-service-query-runner-policy-${var.deployment_stage}"
  role = "${aws_iam_role.query_runner.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "${aws_cloudwatch_log_group.query_runner.arn}:*",
            "Effect": "Allow"
        },
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "${aws_cloudwatch_log_group.query_runner.arn}"
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
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage"
          ],
          "Resource": [
            "arn:aws:sqs:${var.aws_region}:${var.account_id}:dcp-matrix-query-queue-${var.deployment_stage}",
            "arn:aws:sqs:${var.aws_region}:${var.account_id}:dcp-matrix-query-deadletter-queue-${var.deployment_stage}"
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
            "arn:aws:s3:::dcp-matrix-service-queries-${var.deployment_stage}",
            "arn:aws:s3:::dcp-matrix-service-queries-${var.deployment_stage}/*"
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
          "Effect": "Allow",
          "Action": [
            "batch:Describe*",
            "batch:RegisterJobDefinition",
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

resource "aws_ecs_cluster" "query_runner" {
  name = "matrix-service-query-runner-${var.deployment_stage}"
}

resource "aws_ecs_service" "query_runner" {
  name            = "matrix-service-query-runner-${var.deployment_stage}"
  cluster         = "${aws_ecs_cluster.query_runner.id}"
  task_definition = "${aws_ecs_task_definition.query_runner.arn}"
  desired_count   = "${var.query_runner_concurrency}"
  launch_type     = "FARGATE"

  network_configuration {
    security_groups = ["${aws_vpc.vpc.default_security_group_id}"]
    subnets         = ["${data.aws_subnet_ids.matrix_vpc.ids}"]
    assign_public_ip = true
  }
}

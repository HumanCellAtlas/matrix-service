resource "aws_ecs_task_definition" "query_runner" {
  family                = "matrix-service-query-runner-${var.deployment_stage}"
  requires_compatibilities = ["FARGATE"]
  execution_role_arn = "${aws_iam_role.task_executor.arn}"
  task_role_arn = "${aws_iam_role.query_runner.arn}"
  container_definitions = <<DEFINITION
[
  {
    "environment": [],
    "ulimits": [
      {
        "softLimit": 4100,
        "hardLimit": 4100,
        "name": "nofile"
      }
    ],
    "memory": 1024,
    "cpu": 512,
    "image": "humancellatlas/matrix-query-runner:1",
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
  cpu                   = "512"
  memory                = "1024"
}

resource "aws_cloudwatch_log_group" "query_runner" {
  name              = "/aws/service/matrix-service-query-runner-${var.deployment_stage}"
  retention_in_days = 90
}

resource "aws_iam_role" "task_executor" {
  name = "matrix-service-PgbouncerTaskExecutionRole-${var.deployment_stage}"
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
  desired_count   = 0
  launch_type     = "FARGATE"

  network_configuration {
    security_groups = ["${aws_vpc.vpc.default_security_group_id}"]
    subnets         = ["${data.aws_subnet_ids.matrix_vpc.ids}"]
    assign_public_ip = true
  }
}

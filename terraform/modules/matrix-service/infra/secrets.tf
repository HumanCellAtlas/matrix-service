resource "aws_secretsmanager_secret" "infra_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/infra"
}

resource "aws_secretsmanager_secret_version" "infra_secrets" {
  secret_id = "${aws_secretsmanager_secret.infra_secrets.id}"
  secret_string = <<SECRETS_JSON
{
  "query_job_q_arn": "${aws_sqs_queue.query_queue.id}",
}
SECRETS_JSON
}

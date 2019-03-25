resource "aws_secretsmanager_secret" "infra_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/infra"
}

resource "aws_secretsmanager_secret_version" "infra_secrets" {
  secret_id = "${aws_secretsmanager_secret.infra_secrets.id}"
  secret_string = <<SECRETS_JSON
{
  "query_job_q_url": "${aws_sqs_queue.query_queue.id}",
  "query_job_deadletter_q_url": "${aws_sqs_queue.query_deadletter_queue.id}",
  "notification_q_url": "${aws_sqs_queue.notification_queue.id}"
}
SECRETS_JSON
}

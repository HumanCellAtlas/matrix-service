resource "aws_secretsmanager_secret" "infra_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/infra"
}

resource "aws_secretsmanager_secret_version" "infra_secrets" {
  secret_id =  aws_secretsmanager_secret.infra_secrets.id
  secret_string = <<SECRETS_JSON
{
  "query_job_q_url": "${aws_sqs_queue.query_queue.id}",
  "query_job_deadletter_q_url": "${aws_sqs_queue.query_deadletter_queue.id}",
  "gcp_service_acct_creds": "${var.gcp_service_acct_creds}",
  "notification_q_url": "${aws_sqs_queue.notification_queue.id}"
}
SECRETS_JSON
}

resource "aws_secretsmanager_secret" "database_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/database"
}

resource "aws_secretsmanager_secret_version" "database_secrets" {
  secret_id =  aws_secretsmanager_secret.database_secrets.id
  secret_string = <<SECRETS_JSON
{
  "database_uri": "postgresql://${var.redshift_username}:${var.redshift_password}@${aws_redshift_cluster.default.endpoint}/matrix_service_${var.deployment_stage}",
  "readonly_database_uri": "postgresql://${var.readonly_redshift_username}:${var.readonly_redshift_password}@${aws_redshift_cluster.default.endpoint}/matrix_service_${var.deployment_stage}",
  "redshift_role_arn": "${aws_iam_role.matrix_service_redshift.arn}",
  "readonly_username": "${var.readonly_redshift_username}",
  "readonly_password": "${var.readonly_redshift_password}"
}
SECRETS_JSON
}

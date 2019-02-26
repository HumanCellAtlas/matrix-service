resource "aws_secretsmanager_secret" "database_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/database"
}

resource "aws_secretsmanager_secret_version" "database_secrets" {
  secret_id = "${aws_secretsmanager_secret.database_secrets.id}"
  secret_string = <<SECRETS_JSON
{
  "database_uri": "postgresql://${var.redshift_username}:${var.redshift_password}@${aws_redshift_cluster.default.endpoint}/matrix_service_${var.deployment_stage}"
}
SECRETS_JSON
}

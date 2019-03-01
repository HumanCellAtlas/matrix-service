resource "aws_secretsmanager_secret" "database_secrets" {
  name = "dcp/matrix/${var.deployment_stage}/database"
}

resource "aws_secretsmanager_secret_version" "database_secrets" {
  secret_id = "${aws_secretsmanager_secret.database_secrets.id}"
  secret_string = <<SECRETS_JSON
{
  "database_uri": "postgresql://${var.redshift_username}:${var.redshift_password}@${aws_redshift_cluster.default.endpoint}/parth",
  "redshift_role_arn": "${aws_iam_role.matrix_redshift.arn}"
}
SECRETS_JSON
}

locals {
  secrets = {
    merged_mtx_bucket_name = "${var.hca_ms_merged_mtx_bucket}"
    request_status_bucket_name = "${var.hca_ms_request_bucket}"
    sample_matrices_bucket_name = "matrix-service-test"
    sqs_queue_name = "${var.ms_sqs_queue}"
  }
}

resource "aws_secretsmanager_secret" "hca_ms_secrets" {
  name                    = "${var.ms_secret_name}"
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "secrets" {
  secret_id = "${aws_secretsmanager_secret.hca_ms_secrets.id}"
  secret_string = "${jsonencode(local.secrets)}"
}
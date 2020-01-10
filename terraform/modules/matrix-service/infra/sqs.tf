resource "aws_sqs_queue" "query_queue" {
  name                      = "dcp-matrix-query-queue-${var.deployment_stage}"
//  Queue visibility timeout must be larger than (triggered lambda) function timeout
  visibility_timeout_seconds = 1800
  message_retention_seconds = 86400
  redrive_policy            = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.query_deadletter_queue.arn}\",\"maxReceiveCount\":4}"

}

resource "aws_sqs_queue" "query_deadletter_queue" {
  name                      = "dcp-matrix-query-deadletter-queue-${var.deployment_stage}"
  message_retention_seconds = 1209600
}

resource "aws_sqs_queue" "notification_queue" {
  name                      = "dcp-matrix-notification-queue-${var.deployment_stage}"
//  Queue visibility timeout must be larger than (triggered lambda) function timeout
  visibility_timeout_seconds = 1800
  message_retention_seconds = 86400
  redrive_policy            = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.notification_deadletter_queue.arn}\",\"maxReceiveCount\":4}"

}

resource "aws_sqs_queue" "notification_deadletter_queue" {
  name                      = "dcp-matrix-notification-deadletter-queue-${var.deployment_stage}"
  message_retention_seconds = 1209600
}

resource "aws_lambda_event_source_mapping" "notification_source_mapping" {
  batch_size = 1
  event_source_arn  =  aws_sqs_queue.notification_queue.arn
  enabled           = true
  function_name     = "arn:aws:lambda:${var.aws_region}:${var.account_id}:function:dcp-matrix-service-notification-${var.deployment_stage}"
}

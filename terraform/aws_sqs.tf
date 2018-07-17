// Setup SQS

data "template_file" "aws_sqs_queue_policy" {
  template  = "${file("${path.module}/aws_sqs_queue_policy.tpl")}"
  vars {
    sqs_arn = "${aws_sqs_queue.sqs.arn}"
  }
}

resource "aws_sqs_queue" "sqs" {
  name                       = "${var.ms-sqs-queue}"
  visibility_timeout_seconds = 300
}

resource "aws_sqs_queue_policy" "sqs_policy" {
  queue_url = "${aws_sqs_queue.sqs.id}"
  policy    = "${data.template_file.aws_sqs_queue_policy.rendered}"
}
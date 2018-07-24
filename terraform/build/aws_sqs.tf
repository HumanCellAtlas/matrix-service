resource "aws_sqs_queue" "sqs" {
  name                       = "${var.ms_sqs_queue}"
  visibility_timeout_seconds = 300
  redrive_policy             = "{\"deadLetterTargetArn\":\"${aws_sqs_queue.dead_letter_queue.arn}\",\"maxReceiveCount\":3}"
}

resource "aws_sqs_queue" "dead_letter_queue" {
  name = "${var.ms_dead_letter_queue}"
}

resource "aws_sqs_queue_policy" "sqs_policy" {
  queue_url = "${aws_sqs_queue.sqs.id}"
  policy    = <<EOF
{
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.sqs.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sqs_queue.sqs.arn}"
        }
      }
    }
  ]
}
EOF
}
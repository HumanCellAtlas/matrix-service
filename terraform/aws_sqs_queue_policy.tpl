{
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${sqs_arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${sqs_arn}"
        }
      }
    }
  ]
}
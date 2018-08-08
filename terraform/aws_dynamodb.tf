resource "aws_dynamodb_table" "ms_request_status_table" {
  name            = "${var.ms_dynamodb}"
  read_capacity   = 20
  write_capacity  = 20
  hash_key        = "request_id"

  attribute {
    name = "request_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled = true
  }
}
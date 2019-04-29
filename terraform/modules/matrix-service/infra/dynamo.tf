resource "aws_dynamodb_table" "request-table" {
  name           = "dcp-matrix-service-request-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "RequestId"

  attribute {
    name = "RequestId"
    type = "S"
  }
  # Add tags to this resource
}

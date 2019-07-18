resource "aws_dynamodb_table" "request_table" {
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

resource "aws_dynamodb_table" "data_table" {
  name           = "dcp-matrix-service-data-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "Version"
  range_key      = "Date"

  attribute {
    name = "Deployment"
    type = "S"
  }

  attribute {
    name = "Date"
    type = "S"
  }
}

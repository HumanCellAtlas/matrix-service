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

resource "aws_dynamodb_table" "data_version_table" {
  name           = "dcp-matrix-service-data-version-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "DataVersion"

  attribute {
    name = "DataVersion"
    type = "N"
  }
}

resource "aws_dynamodb_table" "deployment_table" {
  name           = "dcp-matrix-service-deployment-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "Deployment"

  attribute {
    name = "Deployment"
    type = "S"
  }
}

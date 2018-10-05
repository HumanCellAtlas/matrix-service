resource "aws_dynamodb_table" "state-table" {
  name           = "dcp-matrix-service-state-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "RequestId"

  attribute {
    name = "RequestId"
    type = "S"
  }
  # Add tags to this resource
}

resource "aws_dynamodb_table" "timing-table" {
  name           = "dcp-matrix-service-timing-table-${var.deployment_stage}"
  read_capacity  = 3
  write_capacity = 3
  hash_key       = "RequestId"

  attribute {
    name = "RequestId"
    type = "S"
  }
  # Add tags to this resource
}

resource "aws_dynamodb_table" "zarr-lock-table" {
  name           = "dcp-matrix-service-zarr-lock-table-${var.deployment_stage}"
  read_capacity  = 25
  write_capacity = 25
  hash_key       = "LockKey"

  attribute {
    name = "LockKey"
    type = "S"
  }
  # Add tags to this resource
}

resource "aws_dynamodb_table" "zarr-output-table" {
  name           = "dcp-matrix-service-zarr-output-table-${var.deployment_stage}"
  read_capacity  = 15
  write_capacity = 15
  hash_key       = "RequestId"

  attribute {
    name = "RequestId"
    type = "S"
  }
  # Add tags to this resource
}

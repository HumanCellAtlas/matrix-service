resource "aws_api_gateway_rest_api" "matrix_service_api_gateway" {
  name        = "matrix-service-api"
  description = "REST API endpoint for matrix service"
}

resource "aws_api_gateway_resource" "matrices" {
  parent_id   = "${aws_api_gateway_rest_api.matrix_service_api_gateway.root_resource_id}"
  path_part   = "matrices"
  rest_api_id = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
}

resource "aws_api_gateway_resource" "health" {
  parent_id   = "${aws_api_gateway_resource.matrices.id}"
  path_part   = "health"
  rest_api_id = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
}

resource "aws_api_gateway_method" "health_check_method" {
  authorization = "NONE"
  http_method   = "GET"
  resource_id   = "${aws_api_gateway_resource.health.id}"
  rest_api_id   = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
}

resource "aws_api_gateway_integration" "matrix_service_api_integration" {
  http_method = "${aws_api_gateway_method.health_check_method.http_method}"
  resource_id = "${aws_api_gateway_resource.health.id}"
  rest_api_id = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
  integration_http_method = "POST"
  type        = "AWS_PROXY"
  uri         = "${aws_lambda_function.matrix_service.invoke_arn}"
}

resource "aws_api_gateway_deployment" "matrix_service_deployment_dev" {
  depends_on  = [
    "aws_api_gateway_method.health_check_method",
    "aws_api_gateway_integration.matrix_service_api_integration"
  ]
  rest_api_id = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
  stage_name  = "dev"
}

resource "aws_api_gateway_deployment" "matrix_service_deployment_prod" {
  depends_on  = [
    "aws_api_gateway_method.health_check_method",
    "aws_api_gateway_integration.matrix_service_api_integration"
  ]
  rest_api_id = "${aws_api_gateway_rest_api.matrix_service_api_gateway.id}"
  stage_name  = "prod"
}

output "matrix_service_dev_url" {
  value = "${aws_api_gateway_deployment.matrix_service_deployment_dev.invoke_url}"
}

output "matrix_service_prod_url" {
  value = "${aws_api_gateway_deployment.matrix_service_deployment_prod.invoke_url}"
}
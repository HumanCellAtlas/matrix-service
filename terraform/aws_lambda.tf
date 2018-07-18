resource "aws_lambda_function" "matrix_service" {
  function_name = "matrix-service"
  s3_bucket     = "${var.hca_ms_deployment_bucket}"
  s3_key        = "deployment.zip"
  description   = "Matrix Service Lambda Function"
  handler       = "app.app"
  role          = "${aws_iam_role.matrix_service_lambda_role.arn}"
  timeout       = 300
  runtime       = "python3.6"
}

resource "aws_lambda_permission" "matrix_service_api_gateway_permisson" {
  action        = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.matrix_service.arn}"
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_deployment.matrix_service_deployment_dev.execution_arn}/*/*"
}
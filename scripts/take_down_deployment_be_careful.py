import argparse

import boto3

lambda_client = boto3.client('lambda')
api_gateway_client = boto3.client('apigateway')
batch_client = boto3.client('batch')

cdn_non_prod_template = "matrix.$DEPLOYMENT_STAGE.data.humancellatlas.org"
cdn_prod = "matrix.data.humancellatlas.org"
lambda_name_templates = [
    "matrix-service-api-$DEPLOYMENT_STAGE",
    "dcp-matrix-service-driver-$DEPLOYMENT_STAGE",
    "dcp-matrix-service-mapper-$DEPLOYMENT_STAGE",
    "dcp-matrix-service-worker-$DEPLOYMENT_STAGE",
    "dcp-matrix-service-reducer-$DEPLOYMENT_STAGE"
]
job_queue_template = "dcp-matrix-converter-queue-$DEPLOYMENT_STAGE"


def main(args):
    # Take down API Gateway
    if args.environment == "prod":
        cdn = cdn_prod
    else:
        cdn = cdn_non_prod_template.replace("$DEPLOYMENT_STAGE", args.environment)
    api_gateway_client.delete_domain_name(
        domainName=cdn
    )
    print(f"took down cdn {cdn}")

    # Throttle all involved lambdas
    for lambda_template in lambda_name_templates:
        lambda_name = lambda_template.replace("$DEPLOYMENT_STAGE", args.environment)
        lambda_client.put_function_concurrency(FunctionName=lambda_name, ReservedConcurrentExecutions=0)
        print(f"halted {lambda_name}")

    # Take down batch job queue
    job_queue_name = job_queue_template.replace("$DEPLOYMENT_STAGE", args.environment)
    batch_client.update_job_queue(jobQueue=job_queue_name, state='DISABLED')
    batch_client.delete_job_queue(jobQueue=job_queue_name)
    print(f"deleted job queue {job_queue_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='take down matrix service deployment')
    parser.add_argument('--environment', help='matrix service environment')
    args = parser.parse_args()
    main(args)

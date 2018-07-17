#!/usr/bin/env bash

export AWS_PROFILE="hca"

rm -rf deployment
cd chalice
chalice package ../deployment/
cd ../deployment/

aws cloudformation package --template-file ./sam.json \
  --s3-bucket hca-dcp-matrix-service-deployment \
  --output-template-file sam-packaged.yaml

aws cloudformation deploy --template-file ./sam-packaged.yaml \
  --stack-name matrix-service-stack \
  --capabilities CAPABILITY_IAM

aws cloudformation describe-stacks --stack-name matrix-service-stack \
    --query "Stacks[].Outputs[?OutputKey=='EndpointURL'][] | [0].OutputValue"

cd ..
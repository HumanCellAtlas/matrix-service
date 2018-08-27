#!/bin/bash
set -exo pipefail

echo "Creating stack named $1"
echo "Uploading to bucket $2"

AWS="aws"

rm -f matrix_service.zip
cd matrix-service-lambdas && zip -X -r ../matrix_service.zip * && cd ..
$AWS s3 cp matrix_service.zip s3://"$2"/matrix_service.zip
$AWS s3 cp matrix-service-api.yaml s3://"$2"/matrix-service-api.yaml

CHANGE_SET="$(python -c "import uuid; print(str(uuid.uuid4()))")"
CHANGE_SET=Z"${CHANGE_SET:1:15}"

response=$($AWS cloudformation create-change-set --stack-name "$1" \
    --change-set-name "$CHANGE_SET" \
    --change-set-type CREATE \
    --template-body file://matrix-service-dummy-cfn.yaml \
    --parameters ParameterKey=LambdaCodeBucket,ParameterValue="$2" \
                 ParameterKey=LambdaCodeKey,ParameterValue=matrix_service.zip \
                 ParameterKey=SwaggerPath,ParameterValue=s3://"$2"/matrix-service-api.yaml \
    --capabilities CAPABILITY_NAMED_IAM)
sleep 15
$AWS cloudformation execute-change-set --change-set-name $(echo "$response" | jq -r '.Id')

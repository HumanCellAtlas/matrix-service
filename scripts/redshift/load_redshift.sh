#!/bin/bash
: '-= Matrix Service Redshift Data Loader =-

Documentation and usage:
https://allspark.dev.data.humancellatlas.org/HumanCellAtlas/matrix-service/wikis/Loading-a-Redshift-Cluster
'

set -euo pipefail

ec2_type="c5d.4xlarge"
max_workers=512
ec2_name=""
state=0
s3_upload_id="N/A"

# read command line inputs
if [ $# -ge 1 ]; then
    export DEPLOYMENT_STAGE="$1"
fi;
if [ $# -ge 2 ]; then
    ec2_type="$2"
fi;
if [ $# -ge 3 ]; then
    max_workers=$3
fi;
if [ $# -ge 4 ]; then
    ec2_name="$4"
fi;
if [ $# -ge 5 ]; then
    state=$5
fi;
if [ $# -ge 6 ]; then
    s3_upload_id=$6
fi;

# read deployment stage
if [ -z ${DEPLOYMENT_STAGE} ]; then
    echo "DEPLOYMENT_STAGE is unset. Exiting"
    exit
fi;

# read account id
if [ $DEPLOYMENT_STAGE = "prod" ]; then
    export AWS_PROFILE="hca-prod"
else
    export AWS_PROFILE="hca"
fi;
echo "Setting AWS_PROFILE to ${AWS_PROFILE}"

export ACCOUNT_ID=$(aws sts get-caller-identity | jq -r .Account)

cd ../../config && source environment && cd ../scripts/redshift
echo "Loading DSS data into Matrix Service ${DEPLOYMENT_STAGE} cluster"

# spin up ec2 instance, provision, execute load_redshift.py
if [ -z "$ec2_name" ]; then
    ec2_name="$(whoami)-$(date +'%Y-%m-%d-%H-%M')"
    aegea launch $ec2_name --instance-type "${ec2_type}" --ami aegea-base5 --wait-for-ssh
    aegea ssh ubuntu@$ec2_name "sudo mkfs -t ext4 /dev/nvme0n1 && sudo mount -t ext4 /dev/nvme0n1 /mnt && df -Th /mnt && sudo chmod 777 /mnt"
fi;
if [ $state -eq 0 ]; then
    aegea ssh ubuntu@$ec2_name "yes | rm -rf /mnt/*"
elif [ $state -eq 1 ]; then
    aegea ssh ubuntu@$ec2_name "yes | rm -rf /mnt/output/*"
fi;
aegea scp -- load_redshift.sh load_redshift.py requirements.txt ../../config/environment ubuntu@$ec2_name:/mnt
aegea scp -- -r ../../matrix/ ubuntu@$ec2_name:/mnt
aegea ssh ubuntu@$ec2_name "cd /mnt && export DEPLOYMENT_STAGE=${DEPLOYMENT_STAGE} && export ACCOUNT_ID=${ACCOUNT_ID} && source environment && sudo pip3 install -U setuptools && sudo pip3 install -r requirements.txt && python3 load_redshift.py --max-workers ${max_workers} --state ${state} --s3-upload-id ${s3_upload_id}"

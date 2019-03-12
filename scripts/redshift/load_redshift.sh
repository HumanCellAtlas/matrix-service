#!/bin/bash
'-= Matrix Service Redshift Data Loader =-

Loads DSS data to a Matrix Service Redshift cluster specified by DEPLOYMENT_STAGE.
- Launches and provisions an I3 AWS EC2 instance
- Executes `load_redshift.py` on instance'

set -euo pipefail

if [ -z ${DEPLOYMENT_STAGE} ]; then
    echo "DEPLOYMENT_STAGE is unset. Exiting"
    exit
else
    stage=${DEPLOYMENT_STAGE}
    account_id=$(aws sts get-caller-identity | jq -r .Account)
    echo "Loading DSS data into Matrix Service ${stage} cluster"
fi;

instance_name="$(whoami)-$(date +'%Y-%m-%d-%H-%M')"
aegea launch $instance_name --instance-type i3.2xlarge --ami aegea-base5 --wait-for-ssh
aegea ssh ubuntu@$instance_name "sudo mkfs -t ext4 /dev/nvme0n1 && sudo mount -t ext4 /dev/nvme0n1 /mnt && df -Th /mnt && sudo chmod 777 /mnt"
aegea ssh ubuntu@$instance_name "yes | rm -rf /mnt/*"
aegea scp -- load_redshift.sh load_redshift.py requirements.txt ../../config/environment ubuntu@$instance_name:/mnt
aegea scp -- -r ../../matrix/ ubuntu@$instance_name:/mnt
aegea ssh ubuntu@$instance_name "cd /mnt && export DEPLOYMENT_STAGE=${stage} && export ACCOUNT_ID=${account_id} && source environment && sudo pip3 install -r requirements.txt && python3 load_redshift.py"

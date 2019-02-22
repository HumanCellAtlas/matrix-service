#!/bin/bash

set -euo pipefail

instance_name="$(whoami)-$(date +'%Y-%m-%d-%H-%M')"
aegea launch $instance_name --instance-type i3.large --ami aegea-base --wait-for-ssh
aegea scp -- -r . ubuntu@$instance_name:~/
aegea ssh ubuntu@$instance_name "sudo export DEPLOYMENT_STAGE=dev && sudo yum install python36 && pip install -r requirements.txt && python3 ~/init_cluster.py"
aegea terminate $instance_name

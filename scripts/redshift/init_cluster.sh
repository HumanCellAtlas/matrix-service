#!/bin/bash

set -euo pipefail

instance_name="$(whoami)-$(date +'%Y-%m-%d-%H-%M')"
aegea launch $instance_name --instance-type i3.2xlarge --ami aegea-base5 --wait-for-ssh
aegea ssh ubuntu@$instance_name "sudo chmod 777 /mnt && sudo mkfs -t ext4 /dev/nvme0n1 && sudo mount -t ext4 /dev/nvme0n1 /mnt && df -Th /mnt"
aegea scp -- -r ./* ubuntu@$instance_name:/mnt
aegea ssh ubuntu@$instance_name "cd /mnt && export DEPLOYMENT_STAGE=dev && sudo pip3 install -r requirements.txt && python3 init_cluster.py"

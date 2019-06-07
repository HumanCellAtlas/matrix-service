import os
import subprocess


def _shell(cmd: str, *args, ret=False):
    cmd_path = subprocess.check_output(["which", cmd]).decode('utf-8')[:-1]
    cmd_list = [cmd_path]
    cmd_list.extend(args)

    if ret:
        return subprocess.check_output(cmd_list).decode('utf-8')[:-1]
    else:
        subprocess.call(cmd_list)


class EC2InstanceManager:
    def __init__(self, name: str):
        self.name = name
        self.ami = "akislyuk-hca-prod-aegea-ami3" if os.environ['DEPLOYMENT_STAGE'] == "prod" else "aegea-base5"
        self.account_id = EC2InstanceManager._fetch_account_id()
        self.deployment_stage = os.environ['DEPLOYMENT_STAGE']

    @staticmethod
    def _fetch_account_id():
        return subprocess.check_output(
            "jq -r .Account",
            stdin=subprocess.Popen("aws sts get-caller-identity",
                                   stdout=subprocess.PIPE,
                                   shell=True).stdout,
            shell=True
        ).decode('utf-8')[:-1]

    def create(self, instance_type):
        _shell("aegea",
               "launch",
               self.name,
               "--instance-type",
               instance_type,
               "--ami",
               self.ami,
               "--wait-for-ssh",
               "--iam-role",
               f"matrix-service-redshift-loader-{self.deployment_stage}")
        _shell("aegea",
               "ssh",
               f"ubuntu@{self.name}",
               "sudo mkfs -t ext2 /dev/nvme0n1 && sudo mount -t ext4 /dev/nvme0n1 /mnt && "
               "df -Th /mnt && sudo chmod 777 /mnt")

    def clear_dir(self, path: str):
        _shell("aegea",
               "ssh",
               f"ubuntu@{self.name}",
               f"yes | rm -rf {path}")

    def provision(self):
        print("Copying src files")
        _shell("aegea",
               "scp",
               "--",
               "launch_loader.py",
               "loader.py",
               "requirements.txt",
               "../../config/environment",
               f"ubuntu@{self.name}:/mnt")
        _shell("aegea",
               "scp",
               "--",
               "-r",
               "../../matrix/",
               f"ubuntu@{self.name}:/mnt")

    def run(self,
            max_workers: int,
            state: int,
            s3_upload_id: str,
            project_uuids: list):
        print("Running loader")
        _shell("aegea",
               "ssh",
               f"ubuntu@{self.name}",
               f"cd /mnt && export DEPLOYMENT_STAGE={self.deployment_stage} && "
               f"export ACCOUNT_ID={self.account_id} && source environment && sudo pip3 install -U setuptools && "
               f"sudo pip3 install -r requirements.txt && python3 loader.py --max-workers {max_workers} "
               f"--state {state} --s3-upload-id {s3_upload_id}"
               + (f" --project-uuids {' '.join(project_uuids)}" if project_uuids else ""))

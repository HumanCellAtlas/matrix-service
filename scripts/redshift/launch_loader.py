import argparse
import datetime
import getpass
import os
import subprocess

from .ec2_instance_manager import EC2InstanceManager


def _init_env_vars():
    os.chdir("../../config")
    subprocess.call("source environment", shell=True)
    os.chdir("../scripts/redshift")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--instance-type",
                        help="Amazon EC2 instance type to create and execute ETL on.",
                        type=str,
                        default="c5d.4xlarge")
    parser.add_argument("--instance-name",
                        help="Existing EC2 instance to execute ETL on.",
                        type=str,
                        default="")
    parser.add_argument("--max-workers",
                        help="Maximum number of concurrent threads during extraction.",
                        type=int,
                        default=512)
    parser.add_argument("--state",
                        help="ETL state (0=pre-ETL, 1=post-E, 2=post-ET, 3=post-ET and upload)",
                        type=int,
                        default=0)
    parser.add_argument("--s3-upload-id",
                        help="Upload UUID in dcp-matrix-service-preload-* S3 bucket "
                             "to load Redshift from (required for state==3).",
                        type=str)
    args = parser.parse_args()

    _init_env_vars()

    if not args.instance_name:
        instance_name = f"{getpass.getuser()}-{datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M')}"
        print(f"No existing instance provided. Spinning up new EC2 {args.instance_type} instance: {instance_name}.")
        ec2_instance = EC2InstanceManager(name=instance_name)
        ec2_instance.create(instance_type=args.instance_type)
    else:
        print(f"Skipping instance creation. Using instance {args.instance_name}.")
        ec2_instance = EC2InstanceManager(name=args.instance_name)

    if args.state == 0:
        print("Clearing all downloaded and generated files.")
        ec2_instance.clear_dir("/mnt/*")
    elif args.state == 1:
        print("Clearing all generated files.")
        ec2_instance.clear_dir("/mnt/output/*")

    ec2_instance.provision()
    ec2_instance.run(max_workers=args.max_workers,
                     state=args.state,
                     s3_upload_id=args.s3_upload_id)

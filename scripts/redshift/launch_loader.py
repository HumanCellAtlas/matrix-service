import argparse
import datetime
import getpass
import os
import subprocess
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from scripts.redshift.ec2_instance_manager import EC2InstanceManager


def _init_env_vars():
    os.chdir("../../config")
    subprocess.call("source environment", shell=True)
    os.chdir("../scripts/redshift")


def launch_loader(args):
    _init_env_vars()

    # spin up a new EC2 instance if necessary
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
                     s3_upload_id=args.s3_upload_id,
                     project_uuids=args.project_uuids,
                     bundle_uuids=args.bundle_uuids)


if __name__ == '__main__':  # pragma: no cover
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--instance-type",
                        help="Amazon EC2 instance type to create and execute ETL on.\n"
                             "One of --instance-type or --instance-name must be supplied.",
                        type=str,
                        default="c5d.4xlarge")
    parser.add_argument("--instance-name",
                        help="Existing EC2 instance to execute ETL on.\n"
                             "One of --instance-type or --instance-name must be supplied.",
                        type=str,
                        default="")
    parser.add_argument("--max-workers",
                        help="Maximum number of concurrent threads to use during extraction (bundle download).",
                        type=int,
                        default=512)
    parser.add_argument("--state",
                        help="Current ETL machine state.\n"
                             "0 = Pre-ETL: executes full ETL\n"
                             "1 = Post-E:  executes transform and load only\n"
                             "2 = Post-ET: executes load (includes upload to S3)\n"
                             "3 = Post-ET: executes load (from S3)",
                        type=int,
                        default=0)
    parser.add_argument("--s3-upload-id",
                        help="REQUIRED for state==3.\n"
                             "S3 prefix (UUID) in dcp-matrix-service-preload-* S3 bucket to load Redshift from.",
                        type=str)
    parser.add_argument("--project-uuids",
                        help="List of DCP Project UUIDs to load into Redshift.\n"
                             "If both project-uuids and bundle-uuids are not supplied, a full ETL will be performed.",
                        type=str,
                        nargs="*",
                        default="")
    parser.add_argument("--bundle-uuids",
                        help="List of DCP Bundle UUIDs to load into Redshift.\n"
                             "If both project-uuids and bundle-uuids are not supplied, a full ETL will be performed.",
                        type=str,
                        nargs="*",
                        default="")
    _args = parser.parse_args()

    launch_loader(_args)

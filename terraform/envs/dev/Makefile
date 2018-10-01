DEPLOYMENT_STAGE=dev
TF_STATE_BUCKET=org-humancellatlas-861229788715-terraform
S3_TFVARS_FILE=s3://$(TF_STATE_BUCKET)/matrix-service/envs/$(DEPLOYMENT_STAGE)/terraform.tfvars

default: plan

init:
	terraform init

plan:
	terraform plan -detailed-exitcode

apply:
	terraform apply --backup=-

retrieve-vars:
	aws s3 cp $(S3_TFVARS_FILE) terraform.tfvars

upload-vars:
	aws s3 cp terraform.tfvars $(S3_TFVARS_FILE)

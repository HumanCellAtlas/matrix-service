# HCA HOST used in the matrix service (Feel free to change it if necessary)
HCA_HOST := https://dss.dev.data.humancellatlas.org/v1

default: all

.PHONY: install
install:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements-dev.txt --upgrade

.PHONY: test
test:
	. venv/bin/activate && PYTHONPATH=chalice \
		python -m unittest discover -s tests -p '*_tests.py'

.PHONY: build
build:
	@read -p "Enter AWS_PROFILE: " aws_profile; \
	read -p "Enter AWS_REGION: " aws_region; \
	read -p "Enter name of s3 bucket which stores deployment package: " deployment_s3_bucket; \
	read -p "Enter name of s3 bucket which stores merged matrices: " merged_mtx_s3_bucket; \
	read -p "Enter name of s3 bucket which stores request status json file: " request_status_s3_bucket; \
	read -p "Enter name of AWS SQS Queue for matrix service: " ms_sqs_queue; \
	read -p "Enter name of secret which stores in aws secret manager: " secret_name; \
	echo "aws_profile = \"$$aws_profile\"\naws_region = \"$$aws_region\"\n\
	hca_ms_merged_mtx_bucket = \"$$merged_mtx_s3_bucket\"\n\
	hca_ms_request_bucket = \"$$request_status_s3_bucket\"\n\
	hca_ms_deployment_bucket = \"$$deployment_s3_bucket\"\n\
	ms_sqs_queue = \"$$ms_sqs_queue\"\n\
	ms_secret_name = \"$$secret_name\"" \
	| tee terraform/build/terraform.tfvars terraform/deploy/terraform.tfvars; \
	echo "{\n\
		\"hca_host\": \"$(HCA_HOST)\",\n\
		\"ms_secret_name\": \"$$secret_name\"\n\
	}" > chalice/chalicelib/config.json
	cd terraform/build && terraform init && terraform apply
	bash -c 'for wheel in vendor.in/*/*.whl; do unzip -q -o -d chalice/vendor/ $$wheel; done'
	. venv/bin/activate && cd chalice && chalice package ../target/ && rm -rf vendor/

.PHONY: deploy
deploy:
	@read -p "Re-enter name of s3 bucket which stores deployment package: " deployment_s3_bucket; \
	read -p "Enter the version number of the service to deploy: " app_version; \
	echo "app_version = \"$$app_version\"" >> terraform/deploy/terraform.tfvars; \
	aws s3 cp target/deployment.zip s3://$$deployment_s3_bucket/v$$app_version/deployment.zip
	cd terraform/deploy && terraform init && terraform apply
	rm -rf target

# Undeploy the lambda functions
.PHONY: clean
clean:
	cd terraform/deploy && terraform destroy
	rm -rf target/

# Undeploy all aws fixtures
.PHONY: clean-all
clean-all:
	cd terraform/deploy && terraform destroy
	cd terraform/build	&& terraform destroy
	rm -rf target
	rm terraform/build/terraform.tfvars
	rm terraform/deploy/terraform.tfvars
	rm -rf venv

.PHONY: all
all:
	make install && make build && make test && make deploy

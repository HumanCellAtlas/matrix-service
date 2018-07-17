# Make sure the value is consistent with the one defined in terraform/variables.tf
DEPLOYMENT_S3_BUCKET := hca-ms-deployment

default: build

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
	bash -c 'for wheel in vendor.in/*/*.whl; do unzip -q -o -d chalice/vendor/ $$wheel; done'
	cd terraform && terraform apply
	. venv/bin/activate && cd chalice && chalice package ../target/
	rm -rf chalice/vendor/

.PHONY: deploy
deploy:
	aws cloudformation package --template-file ./target/sam.json \
	  --s3-bucket $(DEPLOYMENT_S3_BUCKET) \
	  --output-template-file ./target/sam-packaged.yaml
	aws cloudformation deploy --template-file ./target/sam-packaged.yaml \
	  --stack-name matrix-service-stack \
	  --capabilities CAPABILITY_IAM
	aws cloudformation describe-stacks --stack-name matrix-service-stack \
		--query "Stacks[].Outputs[?OutputKey=='EndpointURL'][] | [0].OutputValue"

.PHONY: clean
clean:
	aws cloudformation delete-stack --stack-name matrix-service-stack
	aws cloudformation wait stack-delete-complete --stack-name matrix-service-stack
	rm -rf target/

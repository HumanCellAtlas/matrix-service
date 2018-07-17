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
	cd terraform && terraform apply
	. venv/bin/activate && cd chalice && chalice package ../target/

.PHONY: deploy
deploy:
	aws cloudformation package --template-file ./target/sam.json \
	  --s3-bucket hca-dcp-matrix-service-deployment \
	  --output-template-file ./target/sam-packaged.yaml
	aws cloudformation deploy --template-file ./target/sam-packaged.yaml \
	  --stack-name matrix-service-stack \
	  --capabilities CAPABILITY_IAM
	aws cloudformation describe-stacks --stack-name matrix-service-stack \
		--query "Stacks[].Outputs[?OutputKey=='EndpointURL'][] | [0].OutputValue"

.PHONY: clean
clean:
	cd terraform && terraform destroy
	rm -rf target/

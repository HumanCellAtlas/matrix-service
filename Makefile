default: all

.PHONY: install
install:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements-dev.txt --upgrade

.PHONY: test
test:
	. venv/bin/activate && PYTHONPATH=chalice \
		python -m unittest discover -s tests -p '*_tests.py'

.PHONY: secrets
secrets:
	aws secretsmanager get-secret-value \
		--secret-id matrix-service/dev/terraform.tfvars | \
		jq -r .SecretString | \
		python -m json.tool | \
		tee terraform/build/terraform.tfvars terraform/deploy/terraform.tfvars \
		chalice/chalicelib/config.json

.PHONY: build
build:
	cd terraform/build && terraform init && terraform apply
	bash -c 'for wheel in vendor.in/*/*.whl; do unzip -q -o -d chalice/vendor/ $$wheel; done'
	. venv/bin/activate && cd chalice && chalice package ../target/ && rm -rf vendor/

.PHONY: deploy
deploy:
	@read -p "Enter the version number of the service to deploy: " app_version; \
	aws s3 cp target/deployment.zip s3://$(shell aws secretsmanager get-secret-value --secret-id \
	matrix-service/dev/terraform.tfvars | jq -r .SecretString | jq -r .hca_ms_deployment_bucket)\
	/v$$app_version/deployment.zip
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
	rm chalice/chalicelib/config.json

.PHONY: all
all:
	make install && make secrets && make build && make test && make deploy

include ../common.mk

export EXPORT_ENV_VARS_TO_LAMBDA=DEPLOYMENT_STAGE \
                                    API_HOST \
                                    LAMBDA_DRIVER_FUNCTION_NAME \
                                    LAMBDA_MAPPER_FUNCTION_NAME \
                                    LAMBDA_WORKER_FUNCTION_NAME \
                                    LAMBDA_REDUCER_FUNCTION_NAME \
                                    DYNAMO_STATE_TABLE_NAME \
                                    DYNAMO_OUTPUT_TABLE_NAME \
                                    S3_RESULTS_BUCKET

clean:
	git clean -df .

clobber: clean
	git checkout .chalice/*.json

build:
	mkdir -p chalicelib/matrix/lambdas
	mkdir -p chalicelib/matrix/common
	cp -R ../matrix/lambdas/api chalicelib/matrix/lambdas
	cp ../matrix/common/__init__.py chalicelib/matrix/common
	cp ../matrix/common/constants.py chalicelib/matrix/common
	cp ../matrix/common/exceptions.py chalicelib/matrix/common
	cp ../matrix/common/dynamo_handler.py chalicelib/matrix/common
	cp ../matrix/common/lambda_handler.py chalicelib/matrix/common
	mkdir -p chalicelib/config
	envsubst '$$API_HOST' < ../config/matrix-api.yml > chalicelib/config/matrix-api.yml
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done

deploy: clean build
	@echo -e "\n\n#########################################################"
	@echo -e "########## Deploying to $(DEPLOYMENT_STAGE) environment"
	@echo -e "#########################################################\n"
	./build_deploy_config.sh $(DEPLOYMENT_STAGE)
	chalice deploy --no-autogen-policy --stage $(DEPLOYMENT_STAGE) --api-gateway-stage $(DEPLOYMENT_STAGE)
	rm -rf chalicelib

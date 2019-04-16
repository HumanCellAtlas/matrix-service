include ../common.mk

export EXPORT_ENV_VARS_TO_LAMBDA=DEPLOYMENT_STAGE \
                                 MATRIX_VERSION \
                                 API_HOST \
                                 LAMBDA_DRIVER_FUNCTION_NAME \
                                 LAMBDA_NOTIFICATION_FUNCTION_NAME \
                                 DYNAMO_STATE_TABLE_NAME \
                                 DYNAMO_OUTPUT_TABLE_NAME \
                                 MATRIX_RESULTS_BUCKET \
                                 BATCH_CONVERTER_JOB_QUEUE_ARN \
                                 BATCH_CONVERTER_JOB_DEFINITION_ARN

clean:
	git clean -df .

clobber: clean
	git checkout .chalice/*.json

build:
	mkdir -p chalicelib/matrix/lambdas
	cp -R ../matrix/lambdas/api chalicelib/matrix/lambdas
	cp -R ../matrix/common chalicelib/matrix
	mkdir -p chalicelib/config
	envsubst '$$API_HOST' < ../config/matrix-api.yml > chalicelib/config/matrix-api.yml
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done

deploy: clean build
	@echo -e "\n\n#########################################################"
	@echo -e "########## Deploying to $(DEPLOYMENT_STAGE) environment"
	@echo -e "#########################################################\n"
	./build_deploy_config.sh $(DEPLOYMENT_STAGE)
	chalice deploy --no-autogen-policy --stage $(DEPLOYMENT_STAGE) --api-gateway-stage $(DEPLOYMENT_STAGE)

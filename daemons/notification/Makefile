include ../../common.mk
.PHONY: install build stage deploy clobber

ZIP_FILE=notification_daemon.zip
BUCKET=dcp-matrix-service-lambda-deployment-$(DEPLOYMENT_STAGE)
STAGED_FILE_KEY=$(ZIP_FILE)

default: build

install:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt --upgrade

build:
	rm -rf target
	mkdir target
	pip install -r requirements.txt -t target/ --upgrade --no-compile
	cp target.in/blosc.cpython-36m-x86_64-linux-gnu.so target/numcodecs
	find target/ -name "*.so" | xargs strip -s
	cp -R ../../matrix target/
	cp -R *.py target/

	cd target && zip -r ../$(ZIP_FILE) *

stage: build
	aws s3 cp $(ZIP_FILE) s3://$(BUCKET)/$(STAGED_FILE_KEY)
	rm -rf target

deploy: stage
	aws lambda update-function-code --function-name dcp-matrix-service-notification-$(DEPLOYMENT_STAGE) --s3-bucket $(BUCKET) --s3-key $(STAGED_FILE_KEY)

clobber: ;

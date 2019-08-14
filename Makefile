.PHONY: lint test unit-tests
MODULES=matrix tests daemons chalice
EXCLUDE=target,vendor,chalicelib,target.in

deploy:
	$(MAKE) -C chalice $@
	$(MAKE) -C daemons $@
	cp scripts/dss_subscription.py .
	python dss_subscription.py
	rm dss_subscription.py
	cp scripts/redshift/setup_readonly_user.py .
	python setup_readonly_user.py
	rm setup_readonly_user.py

test: lint unit-tests

lint:
	flake8 $(MODULES) --exclude $(EXCLUDE) *.py --ignore=W503

unit-tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=matrix,scripts --omit=scripts/build_missing_wheels.py,scripts/matrix-service-api.py,scripts/redshift/ec2_instance_manager.py\
		-m unittest discover --start-directory tests/unit --top-level-directory . --verbose

functional-tests:
	PYTHONWARNINGS=ignore:ResourceWarning python \
		-m unittest discover --start-directory tests/functional --top-level-directory . --verbose

load-tests:
	cd tests/locust && locust --host=https://matrix.staging.data.humancellatlas.org --no-web --client=$(NUM_CLIENTS) --hatch-rate=1 --run-time=$(RUN_TIME)

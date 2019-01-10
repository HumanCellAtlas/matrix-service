.PHONY: lint test unit-tests
MODULES=matrix tests daemons chalice
EXCLUDE=target,vendor,chalicelib,target.in

deploy:
	$(MAKE) -C chalice $@
	$(MAKE) -C daemons $@

test: lint unit-tests

lint:
	flake8 $(MODULES) --exclude $(EXCLUDE) *.py

unit-tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=matrix \
		-m unittest discover --start-directory tests/unit --top-level-directory . --verbose

functional-tests:
	PYTHONWARNINGS=ignore:ResourceWarning python \
		-m unittest discover --start-directory tests/functional --top-level-directory . --verbose

load-tests:
	cd tests/locust && locust --host=https://matrix.staging.data.humancellatlas.org --no-web --client=$(NUM_CLIENTS) --hatch-rate=1 --run-time=$(RUN_TIME)

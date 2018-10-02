.PHONY: lint test unit-tests
MODULES=matrix tests daemons chalice
EXCLUDE=target,vendor,chalicelib

deploy:
	$(MAKE) -C chalice $@
	$(MAKE) -C daemons $@

test: lint unit-tests

lint:
	flake8 $(MODULES) --exclude $(EXCLUDE) *.py

unit-tests:
	PYTHONWARNINGS=ignore:ResourceWarning coverage run --source=upload \
		-m unittest discover --start-directory tests/unit --top-level-directory . --verbose

functional-tests:
	PYTHONWARNINGS=ignore:ResourceWarning python \
		-m unittest discover --start-directory tests/functional --top-level-directory . --verbose

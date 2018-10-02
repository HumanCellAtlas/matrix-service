deploy:
	$(MAKE) -C chalice $@
	$(MAKE) -C daemons $@
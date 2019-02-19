HUB_ORG=humancellatlas
IMAGE=matrix-query-runner

# Modus operandi
# --------------
# make build
#	test locally
# make release
#	test with explicit version number from Docker Hub
# make promote
#	promote that latest version to "latest"

build:
	mkdir -p code
	cp -R ../../matrix code/
	cp -R ../../matrix/docker/query_runner.py code
	docker build -t $(IMAGE) .
	rm -rf code/

release: build bump_version push

push:
	$(eval VERSION=$(shell cat VERSION))
	docker tag $(IMAGE):latest $(HUB_ORG)/$(IMAGE):$(VERSION)
	docker push $(HUB_ORG)/$(IMAGE):$(VERSION)

promote:
	$(eval VERSION=$(shell cat VERSION))
	docker tag $(IMAGE):latest $(HUB_ORG)/$(IMAGE):latest
	docker push $(HUB_ORG)/$(IMAGE):latest

bump_version:
	expr `cat VERSION` + 1 > VERSION

.phony: build bump_version promote push release

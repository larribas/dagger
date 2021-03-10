DOCKER_IMAGE_NAME ?= argo_workflows_sdk
VERSION ?= 0.1.0

.PHONY: install
install:
	poetry install

.PHONY: build
build:
	poetry build -f wheel
	docker build . -t $(DOCKER_IMAGE_NAME):$(VERSION) --build-arg "WHEEL=argo_workflows_sdk-$(VERSION)-py3-none-any.whl"

.PHONY: run-%
run-%: build
	docker run -it --entrypoint=$* $(DOCKER_IMAGE_NAME):$(VERSION)

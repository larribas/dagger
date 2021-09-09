DOCKER_IMAGE_NAME ?= dagger
VERSION ?= latest
KUBE_NAMESPACE ?= argo
DIRS ?= dagger/ examples/ tests/

K3D_CLUSTER_NAME ?= dagger
K3D_REGISTRY_NAME ?= local.registry
K3D_REGISTRY_PORT ?= 5000


.PHONY: install
install:
	poetry install

.PHONY: test
test:
	poetry run pytest --cov=dagger --cov-fail-under=98 --cov-report=xml tests/

.PHONY: lint
lint:
	poetry run flake8 $(DIRS)

.PHONY: check-types
check-types:
	poetry run mypy --ignore-missing-imports $(DIRS)

.PHONY: check-docs
check-docs:
	poetry run pydocstyle --explain $(DIRS)

.PHONY: check-format
check-format:
	poetry run black --check --diff $(DIRS)
	poetry run isort --check --diff $(DIRS)

.PHONY: ci
ci: lint check-types check-format check-docs test
	@echo "All checks have passed"

.PHONY: build
build:
	poetry build -f wheel

.PHONY: set-version-from-git
set-version-from-git:
	@echo "Version will be $$(git describe --tags --abbrev=0)"
	poetry version $$(git describe --tags --abbrev=0)
	sed -i "s/0.0.0/$$(git describe --tags --abbrev=0)/" dagger/__init__.py
	cat dagger/__init__.py

.PHONY: docker-build
docker-build: build
	docker build . -t $(DOCKER_IMAGE_NAME):$(VERSION) --build-arg "WHEEL=py_dagger-`poetry version -s`-py3-none-any.whl"

.PHONY: docker-push-local
docker-push-local: docker-build
	docker tag $(DOCKER_IMAGE_NAME):$(VERSION) localhost:$(K3D_REGISTRY_PORT)/$(DOCKER_IMAGE_NAME):$(VERSION)
	docker push localhost:$(K3D_REGISTRY_PORT)/$(DOCKER_IMAGE_NAME):$(VERSION)

.PHONY: run-%
docker-run-%: build
	docker run -it --entrypoint=$* $(DOCKER_IMAGE_NAME):$(VERSION)

.PHONY: set-up-argo
set-up-argo:
	k3d registry create $(K3D_REGISTRY_NAME) --port $(K3D_REGISTRY_PORT)
	k3d cluster create $(K3D_CLUSTER_NAME) --registry-use "k3d-$(K3D_REGISTRY_NAME):$(K3D_REGISTRY_PORT)" --registry-config k3d/registries.yaml --kubeconfig-update-default --kubeconfig-switch-context
	echo "Waiting for a while for the cluster and the namespace to stabilize"
	sleep 10
	kubectl create ns $(KUBE_NAMESPACE)
	kubectl apply -n $(KUBE_NAMESPACE) -k tests/e2e/manifests/argo

.PHONY: tear-down-argo
tear-down-argo:
	k3d registry delete k3d-$(K3D_REGISTRY_NAME)
	k3d cluster delete $(K3D_CLUSTER_NAME)

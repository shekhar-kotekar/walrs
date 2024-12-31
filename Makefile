export CONFIG_FILE_PATH := config.toml

project_name := kraft_rs
k8s_context := kind-kind
IMAGE_REGISTRY := localhost:5001

.PHONY: run set_kind_context dockerize deploy teardown

run:
	cargo run

prepare:
	@if [ -z "$(PACKAGE)" ]; then \
        echo "Error: PACKAGE variable is not set"; \
        exit 1; \
    fi
	@echo "Preparing $(PACKAGE) package"
	cargo fmt && cargo clippy && cargo check

test: prepare
	@echo
	@echo "Running tests for $(PACKAGE) package"
	RUST_LOG=debug cargo test --package $(PACKAGE) -- --nocapture

set_kind_context:
	kubectl config use-context ${k8s_context}
	@echo "INFO: k8s context set to ${k8s_context}"
	@echo

dockerize: set_kind_context
	@echo "INFO: Building docker image."

	GIT_COMMIT=$(git rev-parse --short HEAD)
	@echo "INFO: GIT_COMMIT: ${GIT_COMMIT}"
	# --progress=plain
	docker build --tag ${IMAGE_REGISTRY}/${project_name}:latest -f ./Dockerfile .
	docker push ${IMAGE_REGISTRY}/${project_name}:latest

	@echo "INFO: docker image built successfully!"
	docker images

deploy: dockerize
	@echo
	@echo "INFO: Deploying to k8s cluster"
	kubectl apply -f ./k8s/prerequisites.yml
	kubectl apply -f ./k8s/deployment.yml

	@echo "INFO: Deployed successfully!"
	kubectl get pods --namespace=kraft-rs

redeploy: dockerize
	@echo
	@echo "INFO: Redeploying to k8s cluster"
	kubectl rollout restart deployment/kraft-rs --namespace=kraft-rs

teardown: set_kind_context
	@echo "INFO: Deleting deployment"
	kubectl delete -f ./k8s/deployment.yml
	kubectl delete -f ./k8s/prerequisites.yml

	@echo "INFO: Deleted successfully!"
	kubectl get pods --namespace=kraft-rs

export CONFIG_FILE_PATH := config.toml

export PROJECT_NAME := walrs
k8s_context := kind-kind
IMAGE_REGISTRY := localhost:5001
export GIT_COMMIT := $(shell git rev-parse --short HEAD)

.PHONY: run_server set_kind_context dockerize deploy teardown replace_environment_variables dev-setup

run_server:
	RUSTFLAGS='--cfg tokio_unstable' cargo run --bin walrs_server

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

build_image:
	@echo "INFO: Building docker image."
	# --progress=plain
	docker build --tag ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT} -f ./server/Dockerfile .
	@echo "INFO: docker image built successfully!"

push_image: set_kind_context build_image
	docker push ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT}

replace_environment_variables:
	@echo "INFO: Replacing environment variables in k8s deployment file"
	@echo "DEBUG: git_commit = $(GIT_COMMIT)"

	@mkdir -p ./server/k8s/${GIT_COMMIT}

	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./server/k8s/prerequisites.yml > ./server/k8s/temp/${GIT_COMMIT}/prerequisites.yml

	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./server/k8s/server.yml > ./server/k8s/temp/${GIT_COMMIT}/server.yml

	@echo "INFO: Environment variables replaced successfully!"

deploy:
	@if [ "$(FAST)" = "true" ]; then \
        echo "INFO: Fast mode enabled. Skipping build_image and push_image."; \
        $(MAKE) replace_environment_variables; \
    else \
        $(MAKE) push_image replace_environment_variables; \
    fi
	@echo "INFO: Deploying to ${k8s_context} k8s cluster\n"
	kubectl apply -f ./server/k8s/temp/${GIT_COMMIT}/prerequisites.yml
	kubectl apply -f ./server/k8s/temp/${GIT_COMMIT}/server.yml

	@echo "INFO: Deployed successfully!\n"
	kubectl get pods --namespace=${PROJECT_NAME}

redeploy: replace_environment_variables
	@echo
	@echo "INFO: Redeploying to k8s cluster"
	@kubectl rollout restart statefulset ${PROJECT_NAME}-srvr --namespace=${PROJECT_NAME}

teardown: set_kind_context
	@echo "INFO: Deleting deployment"
	kubectl delete -f ./server/k8s/temp/${GIT_COMMIT}/server.yml
	kubectl delete -f ./server/k8s/temp/${GIT_COMMIT}/prerequisites.yml

	rm -rf ./server/k8s/temp/${GIT_COMMIT}/

	@echo "INFO: Deleted successfully!"
	kubectl get namespaces

dev-setup:
	@cargo install --locked tokio-console
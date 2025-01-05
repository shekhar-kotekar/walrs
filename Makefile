export CONFIG_FILE_PATH := config.toml

export PROJECT_NAME := walrs
k8s_context := kind-kind
IMAGE_REGISTRY := localhost:5001
export GIT_COMMIT := $(shell git rev-parse --short HEAD)

.PHONY: run set_kind_context dockerize deploy teardown replace_environment_variables

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
	# --progress=plain
	docker build --tag ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT} -f ./Dockerfile .
	docker push ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT}

	@echo "INFO: docker image built successfully!"
	docker images

replace_environment_variables: dockerize
	@echo "INFO: Replacing environment variables in k8s deployment file"
	@echo "DEBUG: git_commit = $(GIT_COMMIT)"

	@mkdir -p ./k8s/${GIT_COMMIT}
	
	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./k8s/prerequisites.yml > ./k8s/${GIT_COMMIT}/prerequisites.yml

	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./k8s/statefulset.yml > ./k8s/${GIT_COMMIT}/statefulset.yml
	
	@echo "INFO: Environment variables replaced successfully!"

deploy: replace_environment_variables
	@echo
	@echo "INFO: Deploying to k8s cluster"
	kubectl apply -f ./k8s/${GIT_COMMIT}/prerequisites.yml
	kubectl apply -f ./k8s/${GIT_COMMIT}/statefulset.yml

	@echo "INFO: Deployed successfully!"
	kubectl get pods --namespace=${PROJECT_NAME}

redeploy: dockerize
	@echo
	@echo "INFO: Redeploying to k8s cluster"
	kubectl rollout restart statefulset/${PROJECT_NAME}-broker --namespace=${PROJECT_NAME}

teardown: set_kind_context
	@echo "INFO: Deleting deployment"
	kubectl delete -f ./k8s/${GIT_COMMIT}/statefulset.yml
	kubectl delete -f ./k8s/${GIT_COMMIT}/prerequisites.yml

	rm -rf ./k8s/${GIT_COMMIT}/

	@echo "INFO: Deleted successfully!"
	kubectl get namespaces

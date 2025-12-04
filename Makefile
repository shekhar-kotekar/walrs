k8s_context := kind
IMAGE_REGISTRY := localhost:5001
export PROJECT_NAME := walrs
export GIT_COMMIT := $(shell git rev-parse --short HEAD)

set_kind_context:
	kubectl config use-context ${k8s_context}
	@echo "INFO: k8s context set to ${k8s_context}"
	@echo

build:
	# --progress plain \
	# --platform linux/amd64,linux/arm64 -t walrs_server:latest .
	@echo "DEBUG: GIT_COMMIT = ${GIT_COMMIT}"
	@podman buildx build --platform linux/arm64 -t ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT} .
	@podman images

push_image: set_kind_context build
	podman push --tls-verify=false ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT}

replace_environment_variables: set_kind_context
	@echo "INFO: Replacing environment variables in k8s deployment file"
	@echo "DEBUG: git_commit = $(GIT_COMMIT)"

	@mkdir -p ./server/k8s/temp/${GIT_COMMIT}

	@ls -ltrha ./server/k8s/temp/

	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./server/k8s/prerequisites.yml > ./server/k8s/temp/${GIT_COMMIT}/prerequisites.yml

	@sed -e 's/\$${GIT_COMMIT}/$(GIT_COMMIT)/' \
		 -e 's/\$${PROJECT_NAME}/$(PROJECT_NAME)/' < ./server/k8s/server.yml > ./server/k8s/temp/${GIT_COMMIT}/server.yml

	@echo "INFO: Environment variables replaced successfully!"

deploy: set_kind_context
	@kubectl delete statefulsets.apps walrs-srvr -n walrs || true
	@if [ "$(FAST)" = "true" ]; then \
        echo "INFO: Fast mode enabled. Skipping build_image and push_image."; \
        $(MAKE) replace_environment_variables; \
    else \
        $(MAKE) push_image replace_environment_variables; \
    fi

	@echo "INFO: Deploying to ${k8s_context} k8s cluster\n"
	kubectl apply -f ./server/k8s/temp/${GIT_COMMIT}/prerequisites.yml
	kubectl apply -f ./server/k8s/temp/${GIT_COMMIT}/server.yml
	kubectl apply -f ./server/k8s/debug_pod.yml

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
	kubectl delete -f ./server/k8s/debug_pod.yml
	rm -rf ./server/k8s/temp/*

	@echo "INFO: Deleted successfully!"
	kubectl get namespaces

dev-setup:
	@bash ./scripts/dev-setup.sh
	@cargo install --locked tokio-console

run_single_server: build
	docker run --rm -it -p 8080:8080 ${IMAGE_REGISTRY}/${PROJECT_NAME}:${GIT_COMMIT}

run_client:
	@cargo build -p cli
	@./target/debug/cli
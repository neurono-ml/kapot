BINARY_NAME := $$(cat Cargo.toml | grep name | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
PROJECT_VERSION := $$(cat Cargo.toml | grep version | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
GIT_REFERENCE := $$(git log -1 --pretty=%h)

release:
	git tag v$(PROJECT_VERSION) --force
	git tag $(PROJECT_VERSION) --force 
	git push
	git push --tags --force

image:
	podman build -t ghcr.io/neurono-ml/${BINARY_NAME}:${GIT_REFERENCE} .
	podman tag ghcr.io/neurono-ml/${BINARY_NAME}:${GIT_REFERENCE} ghcr.io/neurono-ml/${BINARY_NAME}

publish:
	# cargo publish -p kapot-cache
	cargo publish -p kapot-core
	cargo publish -p kapot-executor
	cargo publish -p kapot-scheduler
	cargo publish -p kapot
	cargo publish -p kapot-cli

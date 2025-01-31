BINARY_NAME := $$(cat Cargo.toml | grep name | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
PROJECT_VERSION := $$(cat Cargo.toml | grep version | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
GIT_REFERENCE := $$(git log -1 --pretty=%h)

release:
	git push
	git tag v$(PROJECT_VERSION) --force
	git push --tags --force
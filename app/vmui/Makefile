# All these commands must run from repository root.

VMUI_DIR := app/vmui/packages/vmui

gen-logsql-pipes:
	cd $(VMUI_DIR) && npm run gen:logsql-pipes

vmui-package-base-image:
	docker build -t vmui-builder-image -f app/vmui/Dockerfile-build ./app/vmui

vmui-build: vmui-package-base-image
	-$(MAKE) gen-logsql-pipes
	docker run --rm \
		--user $(shell id -u):$(shell id -g) \
		--mount type=bind,src="$(shell pwd)/app/vmui",dst=/build \
		-w /build/packages/vmui \
		--entrypoint=/bin/bash \
		vmui-builder-image -c "npm install && npm run build"

vmui-update: vmui-build
	rm -rf app/vlselect/vmui/* && mv $(VMUI_DIR)/build/* app/vlselect/vmui && rm -rf app/vlselect/vmui/dashboards

vmui-install-dependencies:
	cd $(VMUI_DIR) && npm ci

vmui-lint: vmui-install-dependencies
	cd $(VMUI_DIR) && npm run lint

vmui-typecheck: vmui-install-dependencies
	cd $(VMUI_DIR) && npm run typecheck

vmui-test: vmui-install-dependencies
	cd $(VMUI_DIR) && npm run test

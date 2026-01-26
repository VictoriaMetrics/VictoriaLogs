# All these commands must run from repository root.

vmui-run-npm-command: vmui-package-base-image
	docker run --rm \
		--user $(shell id -u):$(shell id -g) \
		--mount type=bind,src="$(shell pwd)/app/vmui",dst=/build \
		--mount type=bind,src="$(shell pwd)/docs",dst=/docs \
		-w /build/packages/vmui \
		--entrypoint=/bin/bash \
		vmui-builder-image -c "[ \"$$VMUI_SKIP_INSTALL\" = \"true\" ] || npm ci; $(NPM_COMMAND)"

vmui-install:
	NPM_COMMAND="true" $(MAKE) vmui-run-npm-command

vmui-package-base-image:
	docker build -t vmui-builder-image -f app/vmui/Dockerfile-build ./app/vmui

vmui-gen-logsql-pipes:
	NPM_COMMAND="npm run gen:logsql-pipes" $(MAKE) vmui-run-npm-command

vmui-build:
	$(MAKE) vmui-gen-logsql-pipes
	NPM_COMMAND="npm run build" $(MAKE) vmui-run-npm-command

vmui-update: vmui-build
	rm -rf app/vlselect/vmui/* && mv app/vmui/packages/vmui/build/* app/vlselect/vmui && rm -rf app/vlselect/vmui/dashboards

vmui-lint:
	NPM_COMMAND="npm run lint" $(MAKE) vmui-run-npm-command

vmui-typecheck:
	NPM_COMMAND="npm run typecheck" $(MAKE) vmui-run-npm-command

vmui-test:
	NPM_COMMAND="npm run test" $(MAKE) vmui-run-npm-command

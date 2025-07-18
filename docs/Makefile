UID := "$(shell id -u)"
PLATFORM := $(shell uname -m)
DOCKER_PLATFORM := "linux/$(if $(findstring $(PLATFORM),arm64),arm64,amd64)"

# These commands must be run from the VictoriaLogs repository root

docs-image: docs-repo-checkout
	docker build \
		-t vmdocs-docker-package \
		--build-arg UID=$(UID) \
		--platform $(DOCKER_PLATFORM) \
		vmdocs

docs-repo-checkout:
	if [ ! -d vmdocs ]; then \
		git clone --depth 1 git@github.com:VictoriaMetrics/vmdocs vmdocs; \
	fi; \
	cd vmdocs && git checkout origin/main && rm -rf content/victorialogs && git checkout ./content/victorialogs && git pull && cd ..

docs-debug: docs-image
	docker run \
		--rm \
		--name vmdocs-docker-container \
		--platform $(DOCKER_PLATFORM) \
		-p 1313:1313 \
		-v ./docs/victorialogs:/opt/docs/content/victorialogs \
			vmdocs-docker-package

docs-update-version: docs-image
	$(if $(filter v%,$(PKG_TAG)), \
		docker run \
			--rm \
			--entrypoint /usr/bin/find \
			--platform $(DOCKER_PLATFORM) \
			--name vmdocs-docker-container \
			-v ./docs/victorialogs:/opt/docs/content/victorialogs vmdocs-docker-package \
				content \
					-regex ".*\.md" \
					-exec sed -i 's/{{% available_from "#" %}}/{{% available_from "$(PKG_TAG)" %}}/g' {} \;, \
		$(info "Skipping docs version update, invalid $$PKG_TAG: $(PKG_TAG)"))

# Converts images at docs folder to webp format
# See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#images-in-documentation
docs-images-to-webp: docs-image
	docker run \
		--rm \
		--platform $(DOCKER_PLATFORM) \
		--entrypoint /usr/bin/find \
		--name vmdocs-docker-container \
		-v ./docs/victorialogs:/opt/docs/content/victorialogs vmdocs-docker-package \
			content \
				-regex ".*\.\(png\|jpg\|jpeg\)" \
				-exec sh -c 'cwebp -preset drawing -m 6 -o $$(echo {} | cut -f-1 -d.).webp {} && rm -rf {}' {} \;

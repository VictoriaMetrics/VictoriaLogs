UID := "$(shell id -u)"
PLATFORM := $(shell uname -m)
DOCKER_PLATFORM := "linux/$(if $(findstring $(PLATFORM),arm64),arm64,amd64)"

define FLAGS_HEADER
---
build:
  list: never
  publishResources: false
  render: never
sitemap:
  disable: true
---
<!-- The file has to be manually updated during feature work in PR, make docs-update-flags command could be used peridically to ensure the flags in sync. -->
```shellhelp
endef
export FLAGS_HEADER

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

define extract_enterprise_flags
	awk '\
function isFlagHeader(s) {\
	return (s ~ /^[ \t]*-[^ \t]+/);\
}\
function getFlagName(s,    t, a) {\
	t = s;\
	sub(/^[ \t]*-/, "", t);\
	split(t, a, /[ \t]+/);\
	return a[1];\
}\
FNR==NR {\
	if (isFlagHeader($$0)) {\
		seen[getFlagName($$0)] = 1;\
	}\
	next;\
}\
{\
	if (!inFlags) {\
		if (!isFlagHeader($$0)) next;\
		inFlags = 1;\
	}\
	if (isFlagHeader($$0)) {\
		keep = !seen[getFlagName($$0)];\
	}\
	if (keep) print;\
}' "$(1)" "$(2)" >> "$(3)"
endef

docs-update-victoria-logs-flags:
	# ---- victoria-logs
	(cd /tmp/vl-enterprise && make victoria-logs)
	(cd /tmp/vl-opensource && make victoria-logs)
	(cd /tmp/vl-enterprise && ./bin/victoria-logs -help 2>&1) > /tmp/vl-enterprise/victoria_logs_enterprise_flags_tmp.md
	(cd /tmp/vl-opensource && ./bin/victoria-logs -help 2>&1) > /tmp/vl-opensource/victoria_logs_common_flags_tmp.md

	echo "$$FLAGS_HEADER" > docs/victorialogs/victoria_logs_common_flags.md
	cat /tmp/vl-opensource/victoria_logs_common_flags_tmp.md >> docs/victorialogs/victoria_logs_common_flags.md
	printf -- '```\n' >> docs/victorialogs/victoria_logs_common_flags.md

	echo "$$FLAGS_HEADER" > docs/victorialogs/victoria_logs_enterprise_flags.md
	$(call extract_enterprise_flags,/tmp/vl-opensource/victoria_logs_common_flags_tmp.md,/tmp/vl-enterprise/victoria_logs_enterprise_flags_tmp.md,docs/victorialogs/victoria_logs_enterprise_flags.md)
	printf -- '```\n' >> docs/victorialogs/victoria_logs_enterprise_flags.md

	# replace tabs in output with one space
	sed -i.bak 's/\t/ /g' docs/victorialogs/victoria_logs_common_flags.md
	sed -i.bak 's/\t/ /g' docs/victorialogs/victoria_logs_enterprise_flags.md

	# adjust flags with dynamic default values
	sed -i.bak '/Default number of parallel data readers to use for executing every query/ s/(default [0-9]\+)/(default 2x CPU cores)/' docs/victorialogs/victoria_logs_common_flags.md
	sed -i.bak '/The maximum number of concurrent insert requests/ s/(default [0-9]\+)/(default 2x CPU cores)/' docs/victorialogs/victoria_logs_common_flags.md
	sed -i.bak '/The maximum number of concurrent search requests\./ s/(default [0-9]\+)/(default vlselect.getDefaultMaxConcurrentRequests())/' docs/victorialogs/victoria_logs_common_flags.md
	sed -i.bak '/The maximum number of concurrent goroutines to work with files;/ s/(default [0-9]\+)/(default 16x CPU cores, default capped at 256)/' docs/victorialogs/victoria_logs_common_flags.md
	rm -f docs/victorialogs/victoria_logs_common_flags.md.bak docs/victorialogs/victoria_logs_enterprise_flags.md.bak

docs-update-vlagent-flags:
	# ---- vlagent
	(cd /tmp/vl-enterprise && make vlagent)
	(cd /tmp/vl-opensource && make vlagent)
	(cd /tmp/vl-enterprise && ./bin/vlagent -help 2>&1) > /tmp/vl-enterprise/vlagent_enterprise_flags_tmp.md
	(cd /tmp/vl-opensource && ./bin/vlagent -help 2>&1) > /tmp/vl-opensource/vlagent_common_flags_tmp.md

	echo "$$FLAGS_HEADER" > docs/victorialogs/vlagent_common_flags.md
	cat /tmp/vl-opensource/vlagent_common_flags_tmp.md >> docs/victorialogs/vlagent_common_flags.md
	printf -- '```\n' >> docs/victorialogs/vlagent_common_flags.md

	echo "$$FLAGS_HEADER" > docs/victorialogs/vlagent_enterprise_flags.md
	$(call extract_enterprise_flags,/tmp/vl-opensource/vlagent_common_flags_tmp.md,/tmp/vl-enterprise/vlagent_enterprise_flags_tmp.md,docs/victorialogs/vlagent_enterprise_flags.md)
	printf -- '```\n' >> docs/victorialogs/vlagent_enterprise_flags.md

	# replace tabs in output with one space
	sed -i.bak 's/\t/ /g' docs/victorialogs/vlagent_common_flags.md
	sed -i.bak 's/\t/ /g' docs/victorialogs/vlagent_enterprise_flags.md

	# adjust flags with dynamic default values
	sed -i.bak '/The maximum number of concurrent insert requests/ s/(default [0-9]\+)/(default 2x CPU cores)/' docs/victorialogs/vlagent_common_flags.md
	sed -i.bak '/The number of concurrent queues to each -remoteWrite.url/ s/(default [0-9]\+)/(default 2x CPU cores)/' docs/victorialogs/vlagent_common_flags.md
	sed -i.bak '/The maximum number of concurrent goroutines to work with files;/ s/(default [0-9]\+)/(default 16x CPU cores, default capped at 256)/' docs/victorialogs/vlagent_common_flags.md
	rm -f docs/victorialogs/vlagent_common_flags.md.bak docs/victorialogs/vlagent_enterprise_flags.md.bak

docs-update-vlogscli-flags:
	# ---- vlogscli
	(cd /tmp/vl-opensource && make vlogscli)
	(cd /tmp/vl-opensource && ./bin/vlogscli -help 2>&1) > /tmp/vl-opensource/vlogscli_common_flags_tmp.md

	echo "$$FLAGS_HEADER" > docs/victorialogs/querying/vlogscli_common_flags.md
	cat /tmp/vl-opensource/vlogscli_common_flags_tmp.md >> docs/victorialogs/querying/vlogscli_common_flags.md
	printf -- '```\n' >> docs/victorialogs/querying/vlogscli_common_flags.md

	# replace tabs in output with one space
	sed -i.bak 's/\t/ /g' docs/victorialogs/querying/vlogscli_common_flags.md

	# adjust flags with dynamic default values
	sed -i.bak '/The maximum number of concurrent insert requests/ s/(default [0-9]\+)/(default 2x CPU cores)/' docs/victorialogs/querying/vlogscli_common_flags.md
	sed -i.bak '/The maximum number of concurrent goroutines to work with files;/ s/(default [0-9]\+)/(default 16x CPU cores, default capped at 256)/' docs/victorialogs/querying/vlogscli_common_flags.md
	rm -f docs/victorialogs/querying/vlogscli_common_flags.md.bak

# docs-update-flags updates flags in the documentation using the actual binaries compiled
# from the latest enterprise and opensource branches (hardcoded for now).
# The command also normalizes the output a bit.
#
# The command does not replace the need to manually update flags in the documentation when
# new flags are added or existing flags are updated. It just helps to keep the documentation
# in sync with the actual binaries.
#
# It can be run from any branch.
# All work happens inside temporary directories under /tmp.
# The script checks out the required branch, builds the binaries, and updates the documentation.
# The current Git repository is not touched.
docs-update-flags:
	# Note for MacOS users:
	# You need to install gnu versions of sed and awk in order for in-place editing to work
	# Install using: brew install gnu-sed gawk
	# Add tools to PATH see how in `brew info gnu-sed` and `brew info gawk`

	git fetch enterprise
	git fetch opensource

	rm -rf /tmp/vl-enterprise
	git worktree remove /tmp/vl-enterprise || true
	git worktree add /tmp/vl-enterprise enterprise/master

	rm -rf /tmp/vl-opensource
	git worktree remove /tmp/vl-opensource || true
	git worktree add /tmp/vl-opensource opensource/master

	make docs-update-victoria-logs-flags
	make docs-update-vlagent-flags
	make docs-update-vlogscli-flags

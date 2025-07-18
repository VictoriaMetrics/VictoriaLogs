PKG_PREFIX := github.com/VictoriaMetrics/VictoriaLogs

MAKE_CONCURRENCY ?= $(shell getconf _NPROCESSORS_ONLN)
MAKE_PARALLEL := $(MAKE) -j $(MAKE_CONCURRENCY)
DATEINFO_TAG ?= $(shell date -u +'%Y%m%d-%H%M%S')
BUILDINFO_TAG ?= $(shell echo $$(git describe --long --all | tr '/' '-')$$( \
	      git diff-index --quiet HEAD -- || echo '-dirty-'$$(git diff-index -u HEAD | openssl sha1 | cut -d' ' -f2 | cut -c 1-8)))

PKG_TAG ?= $(shell git tag -l --points-at HEAD)
ifeq ($(PKG_TAG),)
PKG_TAG := $(BUILDINFO_TAG)
endif

GO_BUILDINFO = -X 'github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=$(APP_NAME)-$(DATEINFO_TAG)-$(BUILDINFO_TAG)'
TAR_OWNERSHIP ?= --owner=1000 --group=1000

GOLANGCI_LINT_VERSION := 2.2.1

.PHONY: $(MAKECMDGOALS)

include app/*/Makefile
include codespell/Makefile
include docs/Makefile
include deployment/*/Makefile
include dashboards/Makefile
include package/release/Makefile

all: \
	victoria-logs-prod \
	vlagent-prod \
	vlogscli-prod

clean:
	rm -rf bin/*

publish: \
	publish-victoria-logs \
	publish-vlagent \
	publish-vlogscli

package: \
	package-victoria-logs \
	package-vlagent \
	package-vlogscli

vlutils: \
	vlagent \
	vlogscli

vlutils-pure: \
	vlagent-pure \
	vlogscli-pure

vlutils-linux-amd64: \
	vlagent-linux-amd64 \
	vlogscli-linux-amd64

vlutils-linux-arm64: \
	vlagent-linux-arm64 \
	vlogscli-linux-arm64

vlutils-linux-arm: \
	vlagent-linux-arm \
	vlogscli-linux-arm

vlutils-linux-386: \
	vlagent-linux-386 \
	vlogscli-linux-386

vlutils-linux-ppc64le: \
	vlagent-linux-ppc64le \
	vlogscli-linux-ppc64le

vlutils-darwin-amd64: \
	vlagent-darwin-amd64 \
	vlogscli-darwin-amd64

vlutils-darwin-arm64: \
	vlagent-darwin-arm64 \
	vlogscli-darwin-arm64

vlutils-freebsd-amd64: \
	vlagent-freebsd-amd64 \
	vlogscli-freebsd-amd64

vlutils-openbsd-amd64: \
	vlagent-openbsd-amd64 \
	vlogscli-openbsd-amd64

vlutils-windows-amd64: \
	vlagent-windows-amd64 \
	vlogscli-windows-amd64

crossbuild:
	$(MAKE_PARALLEL) victoria-logs-crossbuild vlutils-crossbuild

victoria-logs-crossbuild: \
	victoria-logs-linux-386 \
	victoria-logs-linux-amd64 \
	victoria-logs-linux-arm64 \
	victoria-logs-linux-arm \
	victoria-logs-linux-ppc64le \
	victoria-logs-darwin-amd64 \
	victoria-logs-darwin-arm64 \
	victoria-logs-freebsd-amd64 \
	victoria-logs-openbsd-amd64 \
	victoria-logs-windows-amd64

vlutils-crossbuild: \
	vlutils-linux-386 \
	vlutils-linux-amd64 \
	vlutils-linux-arm64 \
	vlutils-linux-arm \
	vlutils-linux-ppc64le \
	vlutils-darwin-amd64 \
	vlutils-darwin-arm64 \
	vlutils-freebsd-amd64 \
	vlutils-openbsd-amd64 \
	vlutils-windows-amd64

publish-final-images:
	PKG_TAG=$(TAG) APP_NAME=victoria-logs $(MAKE) publish-via-docker-from-rc && \
	PKG_TAG=$(TAG) APP_NAME=vlagent $(MAKE) publish-via-docker-from-rc && \
	PKG_TAG=$(TAG) APP_NAME=vlogscli $(MAKE) publish-via-docker-from-rc && \
	PKG_TAG=$(TAG) $(MAKE) publish-latest

publish-latest:
	PKG_TAG=$(TAG) APP_NAME=victoria-logs $(MAKE) publish-via-docker-latest
	PKG_TAG=$(TAG) APP_NAME=vlogscli $(MAKE) publish-via-docker-latest

publish-release:
	rm -rf bin/*
	git checkout $(TAG) && $(MAKE) release && $(MAKE) publish

release: \
	release-victoria-logs \
	release-vlutils

release-victoria-logs:
	$(MAKE_PARALLEL) release-victoria-logs-linux-386 \
		release-victoria-logs-linux-amd64 \
		release-victoria-logs-linux-arm \
		release-victoria-logs-linux-arm64 \
		release-victoria-logs-darwin-amd64 \
		release-victoria-logs-darwin-arm64 \
		release-victoria-logs-freebsd-amd64 \
		release-victoria-logs-openbsd-amd64 \
		release-victoria-logs-windows-amd64

release-victoria-logs-linux-386:
	GOOS=linux GOARCH=386 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-linux-amd64:
	GOOS=linux GOARCH=amd64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-linux-arm:
	GOOS=linux GOARCH=arm $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-linux-arm64:
	GOOS=linux GOARCH=arm64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-freebsd-amd64:
	GOOS=freebsd GOARCH=amd64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-openbsd-amd64:
	GOOS=openbsd GOARCH=amd64 $(MAKE) release-victoria-logs-goos-goarch

release-victoria-logs-windows-amd64:
	GOARCH=amd64 $(MAKE) release-victoria-logs-windows-goarch

release-victoria-logs-goos-goarch: victoria-logs-$(GOOS)-$(GOARCH)-prod
	cd bin && \
		tar $(TAR_OWNERSHIP) --transform="flags=r;s|-$(GOOS)-$(GOARCH)||" -czf victoria-logs-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			victoria-logs-$(GOOS)-$(GOARCH)-prod \
		&& sha256sum victoria-logs-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			victoria-logs-$(GOOS)-$(GOARCH)-prod \
			| sed s/-$(GOOS)-$(GOARCH)-prod/-prod/ > victoria-logs-$(GOOS)-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf victoria-logs-$(GOOS)-$(GOARCH)-prod

release-victoria-logs-windows-goarch: victoria-logs-windows-$(GOARCH)-prod
	cd bin && \
		zip victoria-logs-windows-$(GOARCH)-$(PKG_TAG).zip \
			victoria-logs-windows-$(GOARCH)-prod.exe \
		&& sha256sum victoria-logs-windows-$(GOARCH)-$(PKG_TAG).zip \
			victoria-logs-windows-$(GOARCH)-prod.exe \
			> victoria-logs-windows-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf \
		victoria-logs-windows-$(GOARCH)-prod.exe

release-vlutils: \
	release-vlutils-linux-386 \
	release-vlutils-linux-amd64 \
	release-vlutils-linux-arm64 \
	release-vlutils-linux-arm \
	release-vlutils-darwin-amd64 \
	release-vlutils-darwin-arm64 \
	release-vlutils-freebsd-amd64 \
	release-vlutils-openbsd-amd64 \
	release-vlutils-windows-amd64

release-vlutils-linux-386:
	GOOS=linux GOARCH=386 $(MAKE) release-vlutils-goos-goarch

release-vlutils-linux-amd64:
	GOOS=linux GOARCH=amd64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-linux-arm64:
	GOOS=linux GOARCH=arm64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-linux-arm:
	GOOS=linux GOARCH=arm $(MAKE) release-vlutils-goos-goarch

release-vlutils-darwin-amd64:
	GOOS=darwin GOARCH=amd64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-darwin-arm64:
	GOOS=darwin GOARCH=arm64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-freebsd-amd64:
	GOOS=freebsd GOARCH=amd64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-openbsd-amd64:
	GOOS=openbsd GOARCH=amd64 $(MAKE) release-vlutils-goos-goarch

release-vlutils-windows-amd64:
	GOARCH=amd64 $(MAKE) release-vlutils-windows-goarch

release-vlutils-goos-goarch: \
	vlagent-$(GOOS)-$(GOARCH)-prod \
	vlogscli-$(GOOS)-$(GOARCH)-prod
	cd bin && \
		tar $(TAR_OWNERSHIP) --transform="flags=r;s|-$(GOOS)-$(GOARCH)||" -czf vlutils-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			vlagent-$(GOOS)-$(GOARCH)-prod \
			vlogscli-$(GOOS)-$(GOARCH)-prod \
		&& sha256sum vlutils-$(GOOS)-$(GOARCH)-$(PKG_TAG).tar.gz \
			vlagent-$(GOOS)-$(GOARCH)-prod \
			vlogscli-$(GOOS)-$(GOARCH)-prod \
			| sed s/-$(GOOS)-$(GOARCH)-prod/-prod/ > vlutils-$(GOOS)-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf \
		vlagent-$(GOOS)-$(GOARCH)-prod \
		vlogscli-$(GOOS)-$(GOARCH)-prod

release-vlutils-windows-goarch: \
	vlagent-windows-$(GOARCH)-prod \
	vlogscli-windows-$(GOARCH)-prod
	cd bin && \
		zip vlutils-windows-$(GOARCH)-$(PKG_TAG).zip \
			vlagent-windows-$(GOARCH)-prod.exe \
			vlogscli-windows-$(GOARCH)-prod.exe \
		&& sha256sum vlutils-windows-$(GOARCH)-$(PKG_TAG).zip \
			vlagent-windows-$(GOARCH)-prod.exe \
			vlogscli-windows-$(GOARCH)-prod.exe \
			> vlutils-windows-$(GOARCH)-$(PKG_TAG)_checksums.txt
	cd bin && rm -rf \
		vlagent-windows-$(GOARCH)-prod.exe \
		vlogscli-windows-$(GOARCH)-prod.exe

pprof-cpu:
	go tool pprof -trim_path=github.com/VictoriaMetrics/VictoriaLogs@ $(PPROF_FILE)

fmt:
	gofmt -l -w -s ./lib
	gofmt -l -w -s ./app
	gofmt -l -w -s ./apptest

vet:
	GOEXPERIMENT=synctest go vet ./lib/...
	go vet ./app/...
	go vet ./apptest/...

check-all: fmt vet golangci-lint govulncheck

clean-checkers: remove-golangci-lint remove-govulncheck

test:
	GOEXPERIMENT=synctest go test ./lib/... ./app/...

test-race:
	GOEXPERIMENT=synctest go test -race ./lib/... ./app/...

test-pure:
	GOEXPERIMENT=synctest CGO_ENABLED=0 go test ./lib/... ./app/...

test-full:
	GOEXPERIMENT=synctest go test -coverprofile=coverage.txt -covermode=atomic ./lib/... ./app/...

test-full-386:
	GOEXPERIMENT=synctest GOARCH=386 go test -coverprofile=coverage.txt -covermode=atomic ./lib/... ./app/...

integration-test: victoria-logs vlagent vlogscli
	go test ./apptest/... -skip="^TestCluster.*"

benchmark:
	GOEXPERIMENT=synctest go test -bench=. ./lib/...
	go test -bench=. ./app/...

benchmark-pure:
	GOEXPERIMENT=synctest CGO_ENABLED=0 go test -bench=. ./lib/...
	CGO_ENABLED=0 go test -bench=. ./app/...

vendor-update:
	go get -u ./lib/...
	go get -u ./app/...
	go mod tidy -compat=1.24
	go mod vendor

app-local:
	CGO_ENABLED=1 go build $(RACE) -ldflags "$(GO_BUILDINFO)" -o bin/$(APP_NAME)$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-pure:
	CGO_ENABLED=0 go build $(RACE) -ldflags "$(GO_BUILDINFO)" -o bin/$(APP_NAME)-pure$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-goos-goarch:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build $(RACE) -ldflags "$(GO_BUILDINFO)" -o bin/$(APP_NAME)-$(GOOS)-$(GOARCH)$(RACE) $(PKG_PREFIX)/app/$(APP_NAME)

app-local-windows-goarch:
	CGO_ENABLED=0 GOOS=windows GOARCH=$(GOARCH) go build $(RACE) -ldflags "$(GO_BUILDINFO)" -o bin/$(APP_NAME)-windows-$(GOARCH)$(RACE).exe $(PKG_PREFIX)/app/$(APP_NAME)

quicktemplate-gen: install-qtc
	qtc

install-qtc:
	which qtc || go install github.com/valyala/quicktemplate/qtc@latest

golangci-lint: install-golangci-lint
	GOEXPERIMENT=synctest golangci-lint run

install-golangci-lint:
	which golangci-lint && (golangci-lint --version | grep -q $(GOLANGCI_LINT_VERSION)) || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v$(GOLANGCI_LINT_VERSION)

remove-golangci-lint:
	rm -rf `which golangci-lint`

govulncheck: install-govulncheck
	govulncheck ./...

install-govulncheck:
	which govulncheck || go install golang.org/x/vuln/cmd/govulncheck@latest

remove-govulncheck:
	rm -rf `which govulncheck`

install-wwhrd:
	which wwhrd || go install github.com/frapposelli/wwhrd@latest

check-licenses: install-wwhrd
	wwhrd check -f .wwhrd.yml

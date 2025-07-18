---
weight: 501
title: Release process guidance for VictoriaLogs
menu:
  docs:
    parent: victorialogs
    identifier: victorialogs-release-process-guidance
    weight: 501
tags: []
aliases:
- /victorialogs/release-guide/index.html
---

## Pre-reqs

### For MacOS users

Make sure you have GNU version of utilities `zip`, `tar`, `sha256sum`. To install them run the following commands:
```sh
brew install coreutils
brew install gnu-tar
export PATH="/usr/local/opt/coreutils/libexec/gnubin:$PATH"
```

Docker may need additional configuration changes:
```sh 
docker buildx create --use --name=qemu
docker buildx inspect --bootstrap  
```

By default, docker on MacOS has limited amount of resources (CPU, mem) to use. 
Bumping the limits may significantly improve build speed.

## Release version and Docker images

1. Make sure all the changes are documented in [CHANGELOG.md](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/docs/victorialogs/CHANGELOG.md).
   Ideally, every change must be documented in the commit with the change. Alternatively, the change must be documented immediately
   after the commit, which adds the change.
1. Run `make vmui-update` command to re-build static files for the [built-in Web UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui)
   and commit the changes to the VictoriaLogs repository.
1. Run `PKG_TAG=v1.xx.y make docs-update-version` command to update version help tooltips
   according to [the contribution guide](https://docs.victoriametrics.com/victoriametrics/contributing/#pull-request-checklist).
1. Cut new version in [CHANGELOG.md](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/docs/victorialogs/CHANGELOG.md)
   and commit it. See example in this [commit](https://github.com/VictoriaMetrics/VictoriaMetrics/commit/b771152039d23b5ccd637a23ea748bc44a9511a7).
1. Make sure you get all changes fetched `git fetch --all`.
1. Create release tag with in the `master` branch with the following command: `git tag -s v1.xx.y`
1. Run `TAG=v1.xx.y make publish-release`. This command performs the following tasks:
   - a) Builds and packages binaries in `*.tar.gz` release archives with the corresponding `_checksums.txt` files inside `bin` directory.
      This step can be run manually with the command `make release` from the needed git tag.
   - b) Build and publish [multi-platform Docker images](https://docs.docker.com/build/buildx/multiplatform-images/) for the given `TAG`.
      The multi-platform Docker image is built for the following platforms:
      * linux/amd64
      * linux/arm64
      * linux/arm
      * linux/ppc64le
      * linux/386
      This step can be run manually with the command `make publish` from the needed git tag.

1. Run `TAG=v1.xx.y make publish-latest` for publishing the `latest` tag for Docker images, which equals to the given `TAG`.

1. Run `TAG=v1.xx.y make github-create-release github-upload-assets`. This command performs the following tasks:

   - a) Create draft GitHub release with the name `TAG`. This step can be run manually
      with the command `TAG=v1.xx.y make github-create-release`.
      The release id is stored at `/tmp/vm-github-release` file.
   - b) Upload all the binaries and checksums created at the previous steps to that release.
      This step can be run manually with the command `make github-upload-assets`.
      It is expected that the needed release id is stored at `/tmp/vm-github-release` file,
      which must be created at the step `a`.
      If the upload process is interrupted by any reason, then the following recovery steps must be performed:
      - To delete the created draft release by running the command `make github-delete-release`.
        This command expects that the id of the release to delete is located at `/tmp/vm-github-release`
        file created at the step `a`.
      - To run the command `TAG=v1.xx.y make github-create-release github-upload-assets`, so new release is created
        and all the needed assets are re-uploaded to it.

1. Push the tag `v1.xx.y` created at the previous steps to public GitHub repository at https://github.com/VictoriaMetrics/VictoriaLogs :

   ```shell
   git push origin v1.xx.y
   ```

1. Go to <https://github.com/VictoriaMetrics/VictoriaLogs/releases> and verify that draft release with the name `TAG` has been created
   and this release contains all the needed binaries and checksums.
1. Update the release description with the contents of [CHANGELOG](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/docs/victorialogs/CHANGELOG.md) for this release.
1. Publish release by pressing "Publish release" button in GitHub's UI.
1. Update GitHub issues related to the new release - they are mentioned in the [CHANGELOG](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/docs/victorialogs/CHANGELOG.md).
   Put human-readable description on how the issue has been addressed and which particular release contains the change.
1. Bump VictoriaLogs version at `deployment/docker/*.yml`. For example:

   ```shell
   for f in $(grep "v1\.116\.0" -R deployment/docker/ -l); do sed -i 's/v1.116.0/v1.117.0/g' $f; done
   ```

1. Bump VictoriaLogs version mentioned in [docs](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7388).

## Operator

The operator repository [https://github.com/VictoriaMetrics/operator/](https://github.com/VictoriaMetrics/operator/)

### Bump the version of images

- Bump the VictoriaLogs version in [file `internal/config/config.go`](https://github.com/VictoriaMetrics/operator/blob/master/internal/config/config.go) with new release version for:
  - `VM_LOGS_VERSION` key in `defaultEnvs` map,
  - `BaseOperatorConf.LogsVersion` default value.
- Run `make docs`.
- Add the dependency to the new release to the tip section in `docs/CHANGELOG.md` ([example](https://github.com/VictoriaMetrics/operator/pull/1355/commits/1d7f4439c359b371b05a06e93f615dbcfb266cf5)).
- Commit and send a PR for review.

## Helm Charts

The helm chart repository [https://github.com/VictoriaMetrics/helm-charts/](https://github.com/VictoriaMetrics/helm-charts/)

### Bump the version of images

> Note that helm charts versioning uses its own versioning scheme. The version of the charts not tied to the version of VictoriaLogs components.

Bump `appVersion` field in `Chart.yaml` with new release version.
Add new line to "Next release" section in `CHANGELOG.md` about version update (the line must always start with "`-`"). Do **NOT** change headers in `CHANGELOG.md`.
Bump `version` field in `Chart.yaml` with incremental semver version (based on the `CHANGELOG.md` analysis). 

Do these updates to the following charts:

1. Update `victoria-logs-single` chart `version` and `appVersion` in [`Chart.yaml`](https://github.com/VictoriaMetrics/helm-charts/blob/master/charts/victoria-logs-single/Chart.yaml)
1. Update `victoria-logs-cluster` chart `version` and `appVersion` in [`Chart.yaml`](https://github.com/VictoriaMetrics/helm-charts/blob/master/charts/victoria-logs-cluster/Chart.yaml)

See commit example [here](https://github.com/VictoriaMetrics/helm-charts/commit/0ec3ab81795cb098d4741451b66886cc6d9be36c).

Once updated, run the following commands:

1. Commit and push changes to `master`.
1. Run "Release" action on Github.
1. Merge new PR *"Automatic update CHANGELOGs and READMEs"* after pipelines are complete.

## Ansible Roles

> Note that ansible playbooks versioning uses its own versioning scheme. The version of the playbooks is not tied to the version of VictoriaLogs components.

1. Update the version of VictoriaLogs components at [https://github.com/VictoriaMetrics/ansible-playbooks](https://github.com/VictoriaMetrics/ansible-playbooks).
1. Commit changes.
1. Create a new tag with `git tag -sm <TAG> <TAG>`.
1. Push the changes with the new tag. This automatically publishes the new versions to galaxy.ansible.com.

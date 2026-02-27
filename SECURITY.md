# Security Policy

## Supported Versions

The following versions of VictoriaLogs receive regular security fixes:

| Version | Supported          |
|---------|--------------------|
| [latest release](https://docs.victoriametrics.com/victorialogs/changelog/) | :white_check_mark: |
| other releases  | :x:                |

See [this page](https://victoriametrics.com/security/)
for more details.

## Software Bill of Materials (SBOM)

Every container{{% available_from "#" %}} image published to `docker.io` and
`quay.io` includes an SPDX SBOM attestation generated
by BuildKit during `docker buildx build`.

### Inspecting the SBOM

```sh
docker buildx imagetools inspect \
  victoriametrics/victoria-logs:<tag> \
  --format "{{ json .SBOM }}"
```

### Scanning with Trivy

```sh
trivy image --sbom-sources oci \
  victoriametrics/victoria-logs:<tag>
```

## Reporting a Vulnerability

Please report any security issues to <security@victoriametrics.com>

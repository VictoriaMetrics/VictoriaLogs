---
weight: 1
title: Quick Start
menu:
  docs:
    parent: victorialogs
    identifier: vl-quick-start
    weight: 1
    title: Quick Start
tags:
  - logs
  - guide
aliases:
- /victorialogs/quick-start.html
- /victorialogs/quick-start/
- /victorialogs/QuickStart.html
- /VictoriaLogs/QuickStart.html
---
It is recommended to read [README](https://docs.victoriametrics.com/victorialogs/)
and [Key Concepts](https://docs.victoriametrics.com/victorialogs/keyconcepts/)
before you start working with VictoriaLogs.

## How to install and run VictoriaLogs

The following options are available:

- [To run pre-built binaries](https://docs.victoriametrics.com/victorialogs/quickstart/#pre-built-binaries)
- [To run Docker image](https://docs.victoriametrics.com/victorialogs/quickstart/#docker-image)
- [To run in Kubernetes with Helm charts](https://docs.victoriametrics.com/victorialogs/quickstart/#helm-charts)
- [To run in Kubernetes with VictoriaMetrics Operator (VLSingle / VLCluster CRDs)](https://docs.victoriametrics.com/operator/resources/)
- [To build VictoriaLogs from source code](https://docs.victoriametrics.com/victorialogs/quickstart/#building-from-source-code)

### Pre-built binaries

Pre-built binaries for VictoriaLogs are available at the [releases](https://github.com/VictoriaMetrics/VictoriaLogs/releases/) page.
Just download the archive for the needed operating system and architecture, unpack it, and run `victoria-logs-prod` from it.

For example, the following commands download VictoriaLogs archive for Linux/amd64, unpack and run it:

```sh
curl -L -O https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v1.43.1/victoria-logs-linux-amd64-v1.43.1.tar.gz
tar xzf victoria-logs-linux-amd64-v1.43.1.tar.gz
./victoria-logs-prod -storageDataPath=victoria-logs-data
```

VictoriaLogs is ready for [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
and [querying](https://docs.victoriametrics.com/victorialogs/querying/) at the TCP port `9428` now!
It has no external dependencies, so it can run in various environments without additional setup or configuration.
VictoriaLogs automatically adapts to the available CPU and RAM resources. It also automatically sets up and creates
the needed indexes during [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/).

See also:

- [How to configure VictoriaLogs](https://docs.victoriametrics.com/victorialogs/quickstart/#how-to-configure-victorialogs)
- [How to ingest logs into VictoriaLogs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/)

### Docker image

You can run VictoriaLogs in a Docker container. It is the easiest way to start using VictoriaLogs.
Here is the command to run VictoriaLogs in a Docker container:

```sh
docker run --rm -it -p 9428:9428 -v ./victoria-logs-data:/victoria-logs-data \
  docker.io/victoriametrics/victoria-logs:v1.43.1 -storageDataPath=victoria-logs-data
```

See also:

- [How to configure VictoriaLogs](https://docs.victoriametrics.com/victorialogs/quickstart/#how-to-configure-victorialogs)
- [How to ingest logs into VictoriaLogs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/)

### Helm charts

You can run VictoriaLogs in a Kubernetes environment
with [VictoriaLogs single](https://docs.victoriametrics.com/helm/victoria-logs-single/)
or [cluster](https://docs.victoriametrics.com/helm/victoria-logs-cluster/) Helm charts.

See also [victoria-logs-collector](https://docs.victoriametrics.com/helm/victoria-logs-collector/) Helm chart for collecting logs
from all the Kubernetes containers and sending them to VictoriaLogs.

### VictoriaMetrics Operator

You can also run VictoriaLogs in Kubernetes using [VictoriaMetrics Operator](https://docs.victoriametrics.com/operator/resources/).

- [`VLSingle` CRD](https://docs.victoriametrics.com/operator/resources/vlsingle/) declaratively defines a single-node VictoriaLogs deployment.
- [`VLCluster` CRD](https://docs.victoriametrics.com/operator/resources/vlcluster/) declaratively defines a VictoriaLogs cluster and lets the Operator manage `vlinsert`, `vlselect` and `vlstorage` components for you.

### Building from source code

Follow these steps to build VictoriaLogs from source code:

- Check out the VictoriaLogs source code:

  ```sh
  git clone https://github.com/VictoriaMetrics/VictoriaLogs
  cd VictoriaLogs
  ```

- Check out a specific commit if needed:

  ```sh
  git checkout <commit-hash-here>
  ```

- Build VictoriaLogs (requires Go to be installed on your computer. See [how to install Go](https://golang.org/doc/install)):

  ```sh
  make victoria-logs
  ```

- Run the built binary:

  ```sh
  bin/victoria-logs -storageDataPath=victoria-logs-data
  ```

VictoriaLogs is ready for [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
and [querying](https://docs.victoriametrics.com/victorialogs/querying/) at the TCP port `9428` now!
It has no external dependencies, so it can run in various environments without additional setup or configuration.
VictoriaLogs automatically adapts to the available CPU and RAM resources. It also automatically sets up and creates
the needed indexes during [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/).

An alternative approach is to build VictoriaLogs inside a Docker builder container. This approach doesn't require Go to be installed,
but it does require Docker on your computer. See [how to install Docker](https://docs.docker.com/engine/install/):

```sh
make victoria-logs-prod
```

This will build the `victoria-logs-prod` executable inside the `bin` folder.

See also:

- [How to configure VictoriaLogs](https://docs.victoriametrics.com/victorialogs/quickstart/#how-to-configure-victorialogs)
- [How to ingest logs into VictoriaLogs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/)

## How to configure VictoriaLogs

VictoriaLogs is configured via command-line flags. All command-line flags have sane defaults,
so there is generally no need to tune them. VictoriaLogs runs smoothly in most environments
without additional configuration.

Pass `-help` to VictoriaLogs in order to see the list of supported command-line flags with their description and default values:

```sh
/path/to/victoria-logs -help
```

VictoriaLogs stores ingested data in the `victoria-logs-data` directory by default. The directory can be changed
via `-storageDataPath` command-line flag. See [Storage](https://docs.victoriametrics.com/victorialogs/#storage) for details.

By default, VictoriaLogs stores [log entries](https://docs.victoriametrics.com/victorialogs/keyconcepts/) with timestamps
in the time range `[now-7d, now]` and drops logs outside this time range
(i.e., a retention of 7 days). See [Retention](https://docs.victoriametrics.com/victorialogs/#retention) for details on controlling retention
for [ingested](https://docs.victoriametrics.com/victorialogs/data-ingestion/) logs.

We recommend setting up monitoring of VictoriaLogs according to [Monitoring](https://docs.victoriametrics.com/victorialogs/#monitoring).

See also:

- [How to ingest logs into VictoriaLogs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/)

## Docker demos

Docker Compose demos for the single-node and cluster versions of VictoriaLogs that include log collection,
monitoring, alerting, and Grafana are available [here](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker#readme).

Docker Compose demos that integrate VictoriaLogs and various log collectors:

- [Filebeat demo](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/filebeat)
- [Fluentbit demo](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/fluentbit)
- [Logstash demo](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/logstash)
- [Vector demo](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/vector)
- [Promtail demo](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/promtail)

You can use the [VictoriaLogs single](https://docs.victoriametrics.com/helm/victoria-logs-single/) or [cluster](https://docs.victoriametrics.com/helm/victoria-logs-cluster/) Helm charts to run the Vector demo in Kubernetes.

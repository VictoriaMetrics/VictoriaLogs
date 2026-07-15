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

- [To run VictoriaLogs single node from a binary](https://docs.victoriametrics.com/victorialogs/quickstart/#pre-built-binaries)
- [To run VictoriaLogs cluster from binaries](https://docs.victoriametrics.com/victorialogs/quickstart/#starting-vl-cluster-from-binaries)
- [To run Docker image](https://docs.victoriametrics.com/victorialogs/quickstart/#docker-image)
- [To run in Kubernetes with Helm charts](https://docs.victoriametrics.com/victorialogs/quickstart/#helm-charts)
- [To run in Kubernetes with VictoriaMetrics Operator (VLSingle / VLCluster CRDs)](https://docs.victoriametrics.com/operator/resources/)
- [To build VictoriaLogs from source code](https://docs.victoriametrics.com/victorialogs/quickstart/#building-from-source-code)

### Starting VictoriaLogs Single Node from a Binary {id="pre-built-binaries"}

1. Download the archive for your OS and architecture from the [releases page](https://github.com/VictoriaMetrics/VictoriaLogs/releases/latest).
For example, on Linux with `amd64` architecture:

```sh
curl -L -O https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v1.51.0/victoria-logs-linux-amd64-v1.51.0.tar.gz
```

2. Extract the archive to /usr/local/bin by running:

```sh
sudo tar xzf victoria-logs-linux-amd64-v1.51.0.tar.gz -C /usr/local/bin
```

3. Create a VictoriaLogs user on the system:

```sh
sudo useradd -s /usr/sbin/nologin victorialogs
```

4. Create a folder for storing VictoriaLogs data:

```sh
sudo mkdir -p /var/lib/victoria-logs && sudo chown -R victorialogs:victorialogs /var/lib/victoria-logs
```

5. Create a Linux Service by running the following:

```sh
sudo bash -c 'cat <<END >/etc/systemd/system/victorialogs.service
[Unit]
Description=VictoriaLogs service
After=network.target

[Service]
Type=simple
User=victorialogs
Group=victorialogs
ExecStart=/usr/local/bin/victoria-logs-prod -storageDataPath=/var/lib/victoria-logs
SyslogIdentifier=victorialogs
Restart=always

PrivateTmp=yes
ProtectHome=yes
NoNewPrivileges=yes

ProtectSystem=full

[Install]
WantedBy=multi-user.target
END'
```

Extra [command-line flags](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags) can be added to the `ExecStart` line.

> Please note, `victorialogs` service is listening on `:9428` for HTTP connections (see `-httpListenAddr` flag).

6. Start and enable the service by running the following command:

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now victorialogs.service
```

7. Check that the service started successfully:

```sh
sudo systemctl status victorialogs.service
```

8. After VictoriaLogs is in `Running` state, verify the [Web UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui) is working
by going to `http://<ip_or_hostname>:9428/select/vmui`.

### Starting VictoriaLogs Cluster from Binaries {id="starting-vl-cluster-from-binaries"}

VictoriaLogs cluster consists of [3 components](https://docs.victoriametrics.com/victorialogs/cluster/#architecture) -
`vlinsert`, `vlselect` and `vlstorage`. All of them share the same single-node VictoriaLogs executable,
and the role of every node is defined by its command-line flags.
It is recommended to run these components in the same private network (for [security reasons](https://docs.victoriametrics.com/victorialogs/#security)),
but on separate physical nodes for the best performance.

On all nodes, you will need to do the following:

1. Download the archive for your OS and architecture from the [releases page](https://github.com/VictoriaMetrics/VictoriaLogs/releases/latest).
For example, on Linux with `amd64` architecture:

```sh
curl -L -O https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v1.51.0/victoria-logs-linux-amd64-v1.51.0.tar.gz
```

2. Extract the archive to /usr/local/bin by running:

```sh
sudo tar xzf victoria-logs-linux-amd64-v1.51.0.tar.gz -C /usr/local/bin
```

3. Create a user account for VictoriaLogs:

```sh
sudo useradd -s /usr/sbin/nologin victorialogs
```

See recommendations for installing each type of cluster component below.

> Please note, every cluster component is listening on `:9428` for HTTP connections by default (see `-httpListenAddr` flag).

#### Installing vlstorage

1. Create a folder for storing `vlstorage` data:

```sh
sudo mkdir -p /var/lib/vlstorage && sudo chown -R victorialogs:victorialogs /var/lib/vlstorage
```

2. Create a Linux Service for `vlstorage` by running the following command:

```sh
sudo bash -c 'cat <<END >/etc/systemd/system/vlstorage.service
[Unit]
Description=VictoriaLogs vlstorage service
After=network.target

[Service]
Type=simple
User=victorialogs
Group=victorialogs
Restart=always
ExecStart=/usr/local/bin/victoria-logs-prod -storageDataPath=/var/lib/vlstorage

PrivateTmp=yes
ProtectHome=yes
NoNewPrivileges=yes
ProtectSystem=full

[Install]
WantedBy=multi-user.target
END'
```

3. Start and Enable `vlstorage`:

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now vlstorage
```

4. Check that the service started successfully:

```sh
sudo systemctl status vlstorage
```

5. After `vlstorage` is in `Running` state, confirm the service is healthy by visiting `http://<ip_or_hostname>:9428/health` link.
It should return `OK`.

#### Installing vlinsert

1. Create a Linux Service for `vlinsert` by running the following command:

```sh
sudo bash -c 'cat <<END >/etc/systemd/system/vlinsert.service
[Unit]
Description=VictoriaLogs vlinsert service
After=network.target

[Service]
Type=simple
User=victorialogs
Group=victorialogs
Restart=always
ExecStart=/usr/local/bin/victoria-logs-prod -storageNode=<list of vlstorages> -select.disable

PrivateTmp=yes
ProtectHome=yes
NoNewPrivileges=yes
ProtectSystem=full

[Install]
WantedBy=multi-user.target
END'
```

Replace `<list of vlstorages>` with comma-separated addresses of previously configured `vlstorage` services
(e.g. `vlstorage-1:9428,vlstorage-2:9428`). See more details in the `-storageNode` flag description
in [cluster docs](https://docs.victoriametrics.com/victorialogs/cluster/#architecture).
The `-select.disable` flag makes this node serve the [insert APIs](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-apis) only.

2. Start and Enable `vlinsert`:

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now vlinsert.service
```

3. Check that the service started successfully:

```sh
sudo systemctl status vlinsert.service
```

4. After `vlinsert` is in `Running` state, confirm the service is healthy by visiting `http://<ip_or_hostname>:9428/health` link.
It should return `OK`.

#### Installing vlselect

1. Create a Linux Service for `vlselect` by running the following command:

```sh
sudo bash -c 'cat <<END >/etc/systemd/system/vlselect.service
[Unit]
Description=VictoriaLogs vlselect service
After=network.target

[Service]
Type=simple
User=victorialogs
Group=victorialogs
Restart=always
ExecStart=/usr/local/bin/victoria-logs-prod -storageNode=<list of vlstorages> -insert.disable

PrivateTmp=yes
ProtectHome=yes
NoNewPrivileges=yes
ProtectSystem=full

[Install]
WantedBy=multi-user.target
END'
```

Replace `<list of vlstorages>` with comma-separated addresses of previously configured `vlstorage` services,
the same way as for `vlinsert`. The `-insert.disable` flag makes this node serve
the [select APIs](https://docs.victoriametrics.com/victorialogs/querying/#http-api) only.

2. Start and Enable `vlselect`:

```sh
sudo systemctl daemon-reload && sudo systemctl enable --now vlselect.service
```

3. Check that the service started successfully:

```sh
sudo systemctl status vlselect.service
```

4. After `vlselect` is in `Running` state, confirm the service is healthy by visiting `http://<ip_or_hostname>:9428/select/vmui` link.
It should open the [Web UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui) page.

See also:

- [How to configure VictoriaLogs](https://docs.victoriametrics.com/victorialogs/quickstart/#how-to-configure-victorialogs)
- [How to ingest logs into VictoriaLogs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/)

### Docker image

You can run VictoriaLogs in a Docker container. It is the easiest way to start using VictoriaLogs.
Here is the command to run VictoriaLogs in a Docker container:

```sh
docker run --rm -it -p 9428:9428 -v ./victoria-logs-data:/victoria-logs-data \
  docker.io/victoriametrics/victoria-logs:v1.51.0 -storageDataPath=victoria-logs-data
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

- If you build VictoriaLogs from source in order to verify some bugfix or feature in [Web UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui),
  then run `make vmui-update` command before the next step. This command requires Docker to be installed on your computer.
  See [how to install Docker](https://docs.docker.com/engine/install/).

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

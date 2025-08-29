# Docker compose environment for VictoriaMetrics

Docker compose environment for VictoriaLogs includes VictoriaLogs components,
[Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/) 
and [Grafana](https://grafana.com/).

For starting the docker-compose environment ensure that you have docker installed and running, and that you have access
to the Internet.
**All commands should be executed from the root directory of [the VictoriaLogs repo](https://github.com/VictoriaMetrics/VictoriaLogs).**

* Metrics:
  * [vmagent](#vmagent)
* Logs:
  * [VictoriaLogs single server](#victorialogs-server)
  * [VictoriaLogs cluster](#victorialogs-cluster)
* [Common](#common-components)
  * [vmauth](#vmauth)
  * [vmalert](#vmalert)
  * [alertmanager](#alertmanager)
  * [Grafana](#grafana)
* [Alerts](#alerts)
* [Troubleshooting](#troubleshooting)

## vmagent

vmagent is used for scraping and pushing time series to VictoriaMetrics instance. 
It accepts Prometheus-compatible configuration with listed targets for scraping:
* [scraping VictoriaLogs single-node](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/prometheus-vl-single.yml) services;
* [scraping VictoriaLogs cluster](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/prometheus-vl-cluster.yml) services;

Web interface link is [http://localhost:8429/](http://localhost:8429/).

## VictoriaLogs server

To spin-up environment with VictoriaLogs run the following command:
```
make docker-vl-single-up
```
_See [compose-vl-single.yml](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/compose-vl-single.yml)_

VictoriaLogs will be accessible on the `--httpListenAddr=:9428` port.
In addition to VictoriaLogs server, the docker compose contains the following components:
* [vector](https://vector.dev/guides/) service for collecting docker logs and sending them to VictoriaLogs;
* `VictoriaMetrics single-node` to collect metrics from all the components;
* [Grafana](#grafana) is configured with [VictoriaLogs datasource](https://github.com/VictoriaMetrics/victorialogs-datasource).
* [vmalert](#vmalert) is configured to query `VictoriaLogs single-node`, and send alerts state
  and recording rules results to `VictoriaMetrics single-node`;
* [alertmanager](#alertmanager) is configured to receive notifications from `vmalert`.

<picture>
  <source srcset="assets/vl-single-server-dark.png" media="(prefers-color-scheme: dark)">
  <source srcset="assets/vl-single-server-light.png" media="(prefers-color-scheme: light)">
  <img src="assets/vl-single-server-light.png" alt="VictoriaLogs single-server deployment" width="500" >
</picture>

To access Grafana use link [http://localhost:3000](http://localhost:3000).

To access [VictoriaLogs UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui)
use link [http://localhost:8427/select/vmui](http://localhost:8427/select/vmui).

Please, also see [how to monitor](https://docs.victoriametrics.com/victorialogs/#monitoring) 
VictoriaLogs installations.

To shutdown environment execute the following command:
```
make docker-vl-single-down
```

See [troubleshooting](#troubleshooting) in case if issues.

## VictoriaLogs cluster

To spin-up environment with VictoriaLogs cluster run the following command:
```
make docker-vl-cluster-up
```
_See [compose-vl-cluster.yml](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/compose-vl-cluster.yml)_

VictoriaLogs cluster environment consists of `vlinsert`, `vlstorage` and `vlselect` components.
`vlinsert` and `vlselect` are available through `vmauth` on port `:8427`.
For example, `vector` pushes logs via `http://vmauth:8427/insert/elasticsearch/` path,
and Grafana queries `http://vmauth:8427` for datasource queries.

The rest of components are available only inside the environment.

In addition to VictoriaLogs cluster, the docker compose contains the following components:
* [vector](https://vector.dev/guides/) service for collecting docker logs and sending them to `vlinsert`;
* [Grafana](#grafana) is configured with [VictoriaLogs datasource](https://github.com/VictoriaMetrics/victorialogs-datasource) and pointing to `vmauth`.
* `VictoriaMetrics single-node` to collect metrics from all the components;
* `vlinsert` forwards ingested data to `vlstorage`
* `vlselect`s are connected to `vlstorage` for querying data;
* [vmauth](#vmauth) balances incoming read and write requests among `vlselect`s and `vlinsert`s;
* [vmalert](#vmalert) is configured to query `vlselect`s, and send alerts state
  and recording rules results to `VictoriaMetrics single-node`;
* [alertmanager](#alertmanager) is configured to receive notifications from `vmalert`.

<picture>
  <source srcset="assets/vl-cluster-dark.png" media="(prefers-color-scheme: dark)">
  <source srcset="assets/vl-cluster-light.png" media="(prefers-color-scheme: light)">
  <img src="assets/vl-cluster-light.png" alt="VictoriaLogs cluster deployment" width="500">
</picture>

To access Grafana use link [http://localhost:3000](http://localhost:3000).

To access [VictoriaLogs UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui)
use link [http://localhost:8427/select/vmui](http://localhost:8427/select/vmui).

Please, also see [how to monitor](https://docs.victoriametrics.com/victorialogs/#monitoring)
VictoriaLogs installations.

To shutdown environment execute the following command:
```
make docker-vl-cluster-down
```

See [troubleshooting](#troubleshooting) in case if issues.

Please see more examples on integration of VictoriaLogs with other log shippers below:
* [filebeat](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/filebeat) 
* [fluentbit](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/fluentbit) 
* [logstash](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/logstash) 
* [promtail](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/promtail) 
* [vector](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/vector)
* [datadog-agent](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/datadog-agent)
* [journald](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/journald)
* [opentelemetry-collector](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/opentelemetry-collector)
* [telegraf](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/telegraf)
* [fluentd](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/fluentd)
* [datadog-serverless](https://github.com/VictoriaMetrics/VictoriaMetrics/tree/master/deployment/docker/victorialogs/datadog-serverless)

# Common components

## vmauth

[vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/) acts as a [load balancer](https://docs.victoriametrics.com/victoriametrics/vmauth/#load-balancing)
to spread the load across `vlselect` nodes. [Grafana](#grafana) and [vmalert](#vmalert) use vmauth for read queries.
vmauth routes read queries to VictoriaLogs depending on requested path.
vmauth config is available here for [VictoriaLogs single-server](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/auth-vl-single.yml) and
[VictoriaLogs cluster](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/auth-vl-cluster.yml).


## vmalert

vmalert evaluates various [alerting rules](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/rules).
It is connected with AlertManager for firing alerts, and with VictoriaLogs for executing queries and storing alert's state.

Web interface link [http://localhost:8880/](http://localhost:8880/).

## alertmanager

AlertManager accepts notifications from `vmalert` and fires alerts.
All notifications are blackholed according to [alertmanager.yml](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/alertmanager.yml) config.

Web interface link [http://localhost:9093/](http://localhost:9093/).

## Grafana

Web interface link [http://localhost:3000](http://localhost:3000).

Default credentials:
* login: `admin`
* password: `admin`

Grafana is provisioned with default dashboards and datasources.

## Alerts

See below a list of recommended alerting rules for VictoriaLogs components for running in production.
Some alerting rules thresholds are just recommendations and could require an adjustment.
The list of alerting rules is the following:
* [alerts-health.yml](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/rules/alerts-health.yml):
  alerting rules related to all VictoriaMetrics components for tracking their "health" state;
* [alerts-vlogs.yml](https://github.com/VictoriaMetrics/VictoriaMetrics/blob/master/deployment/docker/rules/alerts-vlogs.yml):
  alerting rules related to [VictoriaLogs](https://docs.victoriametrics.com/victorialogs/);

Please, also see [how to monitor VictoriaLogs installations](https://docs.victoriametrics.com/victorialogs/#monitoring).

## Troubleshooting

This environment has the following requirements:
* installed [docker compose](https://docs.docker.com/compose/);
* access to the Internet for downloading docker images;
* **All commands should be executed from the root directory of [the VictoriaMetrics repo](https://github.com/VictoriaMetrics/VictoriaMetrics).**

Containers are started in [--detach mode](https://docs.docker.com/reference/cli/docker/compose/up/), meaning they run in the background. 
As a result, you won't see their logs or exit status directly in the terminal.

If something isn’t working as expected, try the following troubleshooting steps:
1. Run from the correct directory. Make sure you're running the command from the root of the [VictoriaMetrics repository](https://github.com/VictoriaMetrics/VictoriaMetrics).
2. Check container status. Run `docker ps -a` to list all containers and their status. Healthy and running containers should have `STATUS` set to `Up`.
3. View container logs. To inspect logs for a specific container, get its container ID from step p2 and run: `docker logs -f <containerID>`.
4. Read the logs carefully and follow any suggested actions.
5. Check for port conflicts. Some containers (e.g., Grafana) expose HTTP ports. If a port (like `:3000`) is already in use, the container may fail to start. Stop the conflicting process or change the exposed port in the Docker Compose file.
6. Shut down the deployment. To tear down the environment, run: `make <environment>-down` (i.e. `make docker-vl-single-down`). 
   Note, this command also removes all attached volumes, so all the temporary created data will be removed too (i.e. Grafana dashboards or collected metrics).

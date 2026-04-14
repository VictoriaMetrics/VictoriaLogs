# Docker compose OpenTelemetry collector integration with VictoriaLogs 

The folder contains examples of [OpenTelemetry collector](https://opentelemetry.io/docs/collector/) integration with VictoriaLogs using protocols:

* [loki](./loki)
* [otlp](./otlp)
* [syslog](./syslog)
* [elasticsearch single node](./elasticsearch)

## Quick start

To spin-up environment `cd` to any of listed above directories run the following command:
```sh
docker compose up -d 
```

To shut down the docker-compose environment run the following command:
```sh
docker compose down -v
```

The docker compose file contains the following components:

* collector - logs collection agent configured to collect and write data to `vlagent`
* vlagent - agent, that replicates log data to `victorialogs-x` instances
* victorialogs-x - logs database instances, receive data from `vlagent` agent
* vmauth - proxy that serves as a query entrypoint for `victorialogs-x` instances
* victoriametrics - metrics database, which collects metrics from `collector` for observability purposes

## Querying

* [vmui](https://docs.victoriametrics.com/victorialogs/querying/#vmui) - a web UI is accessible by `http://localhost:9428/select/vmui/`
* for querying the data via command-line please check [vlogscli](https://docs.victoriametrics.com/victorialogs/querying/#command-line)

OpenTelemetry collector configuration example can be found below:
* [loki](./loki/config.yml)
* [otlp](./otlp/config.yml)
* [syslog](./syslog/config.yml)
* [elasticsearch single node](./elasticsearch/config.yml)

> Please, note that `_stream_fields` parameter must follow recommended [best practices](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) to achieve better performance.

# Docker compose Serverless with DataDog extension integration with VictoriaLogs

The folder contains examples of [DataDog serverless](https://docs.datadoghq.com/serverless) integration with VictoriaLogs for:

* [AWS Lambda](./aws)
* [GCP Cloud Run](./gcp)

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

* vmauth - VMAuth proxy, with path-based routing to `victoriametrics` and `vlagent`
* vlagent - agent, that replicates log data to `victorialogs-x` instances
* lambda - Serverless application with Datadog logs collection extension, which is configured to collect and write data to `victorialogs-x` and `victoriametrics` via `vmauth`
* victorialogs-x - VictoriaLogs log database, which accepts the data from `lambda`
* victoriametrics - VictoriaMetrics metrics database, collects metrics from `lambda` for observability purposes

## Querying

* [vmui](https://docs.victoriametrics.com/victorialogs/querying/#vmui) - a web UI is accessible by `http://localhost:9428/select/vmui/`
* for querying the data via command-line please check [vlogscli](https://docs.victoriametrics.com/victorialogs/querying/#command-line)

> Please, note that `_stream_fields` parameter must follow recommended [best practices](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) to achieve better performance.

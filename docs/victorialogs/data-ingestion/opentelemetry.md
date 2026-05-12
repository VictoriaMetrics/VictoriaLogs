---
weight: 4
title: OpenTelemetry Setup
disableToc: true
menu:
  docs:
    parent: "victorialogs-data-ingestion"
    weight: 4
tags:
  - logs
aliases:
  - /victorialogs/data-ingestion/OpenTelemetry.html
  - /VictoriaLogs/data-ingestion/OpenTelemetry.html
---
VictoriaLogs supports both client open-telemetry [SDK](https://opentelemetry.io/docs/languages/) and [collector](https://opentelemetry.io/docs/collector/).

## Client SDK

Specify `EndpointURL` for http-exporter builder to `/insert/opentelemetry/v1/logs`.

Consider the following example for Go SDK:

```go
logExporter, err := otlploghttp.New(ctx,
  otlploghttp.WithEndpointURL("http://victorialogs:9428/insert/opentelemetry/v1/logs"),
)
```

VictoriaLogs treats all the resource labels as [log stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields).
The list of log stream fields can be overridden via `VL-Stream-Fields` HTTP header if needed. For example, the following config uses only `host` and `app`
labels as log stream fields, while the remaining labels are stored as [regular log fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model):

```go
logExporter, err := otlploghttp.New(ctx,
  otlploghttp.WithEndpointURL("http://victorialogs:9428/insert/opentelemetry/v1/logs"),
  otlploghttp.WithHeaders(map[string]string{
    "VL-Stream-Fields": "host,app",
  }),
)
```

VictoriaLogs supports other HTTP headers - see the list [here](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers).

The ingested log entries can be queried according to [these docs](https://docs.victoriametrics.com/victorialogs/querying/).

## Field prefixes

By default, VictoriaLogs flattens all OpenTelemetry field sources into a single namespace.
This means the following sources can produce fields with identical names:

| Source | Example OTel path | Stored field name (default) |
|---|---|---|
| Resource attributes | `resource.attributes["service.name"]` | `service.name` |
| Log record attributes | `logRecord.attributes["service.name"]` | `service.name` |
| Log record body (KV list) | `logRecord.body["service.name"]` | `service.name` |
| Generated fields | `logRecord.traceID` | `trace_id` |

When two sources share a key, the log entry ends up with **duplicate field names**, which are
handled inconsistently across query and storage paths. This is especially noticeable when the
collision involves resource attributes, because those are used as
[stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) by default.

### Enabling prefixes

Start VictoriaLogs with the `-opentelemetry.enableFieldPrefixes` flag to add a source prefix
before each field is stored:

```sh
victoria-logs -opentelemetry.enableFieldPrefixes
```

With the flag enabled, each source writes to its own sub-namespace:

| Source | Example OTel path | Stored field name (with flag) |
|---|---|---|
| Resource attributes | `resource.attributes["service.name"]` | `resource.service.name` |
| Log record attributes | `logRecord.attributes["service.name"]` | `attributes.service.name` |
| Log record body (KV list) | `logRecord.body["service.name"]` | `body.service.name` |
| Generated fields | `logRecord.traceID` | `trace_id` *(unchanged)* |
| Scope attributes | `scope.attributes["abc"]` | `scope.attributes.abc` *(unchanged)* |

Scalar body values (string, int, bool, …) continue to arrive in the
[`_msg`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field) field, the same as without the flag.

The flag is **disabled by default** to avoid breaking existing ingestion pipelines.
Enable it for new deployments, or for existing ones after updating any queries and dashboards
that reference the old unprefixed field names.

### Updating stream fields

When field prefixes are enabled, resource attributes are stored as `resource.*` fields.
Auto-selection of stream fields continues to work without changes — VictoriaLogs will
use the prefixed names automatically.

If you have an explicit `VL-Stream-Fields` header that references resource attribute names,
update those names to include the `resource.` prefix. For example:

```go
logExporter, err := otlploghttp.New(ctx,
  otlploghttp.WithEndpointURL("http://victorialogs:9428/insert/opentelemetry/v1/logs"),
  otlploghttp.WithHeaders(map[string]string{
    "VL-Stream-Fields": "resource.host,resource.service.name",
  }),
)
```

## Collector configuration

VictoriaLogs supports receiving logs from the following OpenTelemetry collectors:

* [Elasticsearch](https://docs.victoriametrics.com/victorialogs/data-ingestion/opentelemetry/#elasticsearch)
* [OpenTelemetry](https://docs.victoriametrics.com/victorialogs/data-ingestion/opentelemetry/#opentelemetry)

### Elasticsearch

```yaml
exporters:
  elasticsearch:
    endpoints:
      - http://victorialogs:9428/insert/elasticsearch
receivers:
  filelog:
    include: [/tmp/logs/*.log]
    resource:
      region: us-east-1
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [elasticsearch]
```

If Elasticsearch stores the log message in the field other than [`_msg`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field),
then it can be moved to `_msg` field by using the `VL-Msg-Field` HTTP header. For example, if the log message is stored in the `Body` field,
then it can be moved to `_msg` field via the following config:

```yaml
exporters:
  elasticsearch:
    endpoints:
      - http://victorialogs:9428/insert/elasticsearch
    headers:
      VL-Msg-Field: Body
```

VictoriaLogs supports other HTTP headers - see the list [here](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers).

### OpenTelemetry

Specify logs endpoint for [OTLP/HTTP exporter](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/otlphttpexporter/README.md) in configuration file
for sending the collected logs to VictoriaLogs:

```yaml
exporters:
  otlphttp:
    logs_endpoint: http://localhost:9428/insert/opentelemetry/v1/logs
```

VictoriaLogs supports various HTTP headers, which can be used during data ingestion - see the list [here](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers).
These headers can be passed to OpenTelemetry exporter config via `headers` options. For example, the following config instructs ignoring `foo` and `bar` fields during data ingestion:

```yaml
exporters:
  otlphttp:
    logs_endpoint: http://localhost:9428/insert/opentelemetry/v1/logs
    headers:
      VL-Ignore-Fields: foo,bar
```

See also:

* [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
* [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).
* [Docker-compose demo for OpenTelemetry collector integration with VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/opentelemetry-collector).

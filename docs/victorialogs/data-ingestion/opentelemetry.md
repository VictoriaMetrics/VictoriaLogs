---
weight: 4
title: OpenTelemetry Setup
description: "Configure OpenTelemetry SDKs and Collectors to send logs to VictoriaLogs."
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

VictoriaLogs uses the log body as the [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field).
The log message can be taken from other fields via `VL-Msg-Field` HTTP header if needed. In this case the log body is stored in the `body` field{{% available_from "v1.54.0" %}}.
For example, the following config uses the `msg` log record attribute as the log message, and falls back to the log body if the `msg` attribute is missing:

```go
logExporter, err := otlploghttp.New(ctx,
  otlploghttp.WithEndpointURL("http://victorialogs:9428/insert/opentelemetry/v1/logs"),
  otlploghttp.WithHeaders(map[string]string{
    "VL-Msg-Field": "msg,body",
  }),
)
```

VictoriaLogs supports other HTTP headers - see the list [here](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers).

The ingested log entries can be queried according to [these docs](https://docs.victoriametrics.com/victorialogs/querying/).

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

## Field names

By default, VictoriaLogs stores resource attributes and log record attributes without a prefix. This works well in most cases,
but attributes from different sources may cause field name collisions. See [this issue](https://github.com/VictoriaMetrics/VictoriaLogs/issues/1371).
So it is recommended to set the `-opentelemetry.consistentPrefix` command-line flag{{% available_from "#" %}} for new installations.
The flag is disabled by default for compatibility with already ingested logs, since it changes field names.

For example, the following OpenTelemetry log record (shown in JSON format for readability):

```json
{
  "resourceLogs": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]},
    "scopeLogs": [{
      "scope": {"name": "payment", "version": "2.4.1", "attributes": [{"key": "lib.language", "value": {"stringValue": "go"}}]},
      "logRecords": [{
        "severityNumber": 17,
        "severityText": "ERROR",
        "traceId": "5b8efff798038103d269b633813fc60c",
        "spanId": "eee19b7ec3c1b174",
        "body": {"stringValue": "payment failed"},
        "attributes": [{"key": "http.method", "value": {"stringValue": "POST"}}]
      }]
    }]
  }]
}
```

is stored with the following fields by default:

```json
{
  "service.name": "checkout",
  "scope.name": "payment",
  "scope.version": "2.4.1",
  "scope.attributes.lib.language": "go",
  "_msg": "payment failed",
  "http.method": "POST",
  "trace_id": "5b8efff798038103d269b633813fc60c",
  "span_id": "eee19b7ec3c1b174",
  "severity_number": "17",
  "severity_text": "ERROR"
}
```

and with the following fields when `-opentelemetry.consistentPrefix` is set:

```json
{
  "resource.attribute.service.name": "checkout",
  "scope.name": "payment",
  "scope.version": "2.4.1",
  "scope.attribute.lib.language": "go",
  "_msg": "payment failed",
  "log.attribute.http.method": "POST",
  "trace_id": "5b8efff798038103d269b633813fc60c",
  "span_id": "eee19b7ec3c1b174",
  "severity_number": "17",
  "severity_text": "ERROR"
}
```

Fields of a key-value log body are stored as `<key>` by default and as `body.<key>` with the flag.

The flag also helps to align field names with VictoriaMetrics and VictoriaTraces later, when they follow the same naming convention for OpenTelemetry data. See [this issue](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/11542).

See also:

* [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
* [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).
* [Docker-compose demo for OpenTelemetry collector integration with VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/opentelemetry-collector).

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

By default, VictoriaLogs stores resource attributes, log record attributes and fields of a key-value log body without a prefix.
For example, the `service.name` resource attribute is stored in the `service.name` field. This keeps field names short, but it causes two problems:

- the same attribute may have different names in VictoriaLogs, VictoriaMetrics and VictoriaTraces, so it is harder to correlate logs with metrics and traces;
- attributes from different sources may have the same name. For example, a resource attribute, a log record attribute and a log body field
  may all be named `service.name`, or a log record attribute may be named `trace_id` like the built-in `trace_id` field.
  Then some of these values are lost. See [this issue](https://github.com/VictoriaMetrics/VictoriaLogs/issues/1371).

The `-opentelemetry.consistentPrefix`{{% available_from "#" %}} command-line flag solves both problems for logs ingested via `/insert/opentelemetry/v1/logs`.
It adds a prefix to every attribute name, so the field name shows where the attribute comes from:

- resource attributes are stored as `resource.attribute.<key>` instead of `<key>`;
- scope attributes are stored as `scope.attribute.<key>` instead of `scope.attributes.<key>`;
- log record attributes are stored as `log.attribute.<key>` instead of `<key>`;
- fields of a key-value log body are stored as `body.<key>` instead of `<key>`.

VictoriaMetrics and VictoriaTraces use the same field names when they run with the same flag.

The flag doesn't change the names of the fields that VictoriaLogs creates from the built-in OpenTelemetry fields,
such as `scope.name`, `scope.version`, `trace_id`, `span_id`, `severity_number`, `severity_text` and `event_name`.
Other log body values, such as strings, are still stored in the [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field).

For example, the following log record:

```json
{
  "resourceLogs": [{
    "resource": {
      "attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]
    },
    "scopeLogs": [{
      "scope": {
        "name": "payment",
        "version": "2.4.1",
        "attributes": [{"key": "version", "value": {"stringValue": "1.0"}}]
      },
      "logRecords": [{
        "severityNumber": 17,
        "severityText": "ERROR",
        "body": {"stringValue": "payment failed"},
        "attributes": [{"key": "service.name", "value": {"stringValue": "payment-lib"}}]
      }]
    }]
  }]
}
```

is stored with the following fields when `-opentelemetry.consistentPrefix` is set:

```json
{
  "resource.attribute.service.name": "checkout",
  "scope.name": "payment",
  "scope.version": "2.4.1",
  "scope.attribute.version": "1.0",
  "_msg": "payment failed",
  "log.attribute.service.name": "payment-lib",
  "severity_number": "17",
  "severity_text": "ERROR"
}
```

It is recommended to set the flag before ingesting OpenTelemetry logs, since it changes field names, including the names of
[log stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields). Logs ingested before that keep the old field names,
so queries over both old and new logs must use both names. If you use `VL-Stream-Fields`, `VL-Msg-Field`, `VL-Ignore-Fields`
or `VL-Decolorize-Fields` [HTTP headers](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers),
then update them to the new field names, for example `VL-Stream-Fields: resource.attribute.service.name` or `VL-Msg-Field: body.msg`.

The prefixes make field names longer. Note that VictoriaLogs drops log entries with field names longer than 128 bytes,
see [these docs](https://docs.victoriametrics.com/victorialogs/faq/#what-is-the-maximum-supported-field-name-length).

Set the flag on every component that accepts OpenTelemetry data: single-node VictoriaLogs, all the `vlinsert` nodes
in [VictoriaLogs cluster](https://docs.victoriametrics.com/victorialogs/cluster/) and [vlagent](https://docs.victoriametrics.com/victorialogs/vlagent/).
If some of these components run without the flag, then the stored field names depend on which component receives the logs.

See also:

* [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
* [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).
* [Docker-compose demo for OpenTelemetry collector integration with VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/opentelemetry-collector).

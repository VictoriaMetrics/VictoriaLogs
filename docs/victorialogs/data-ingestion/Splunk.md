---
weight: 14
title: Splunk Setup
disableToc: true
menu:
  docs:
    parent: "victorialogs-data-ingestion"
    weight: 14
tags:
  - logs
---

VictoriaLogs accpets logs via [Splunk HEC API](https://help.splunk.com/en/splunk-enterprise/get-started/get-data-in/9.0/get-data-with-http-event-collector/set-up-and-use-http-event-collector-in-splunk-web) at `/insert/splunk/services/collector/event` or `/insert/splunk/services/collector/event/1.0` HTTP paths.
Additionally paths without `/insert/splunk/` prefix are supported to simplify integration.

## Collect docker logs using Splunk driver

Docker Splunk driver can be configured to send data to VictoriaLogs without any additional agent:

```
services:
  nginx:
    image: nginx:1.27
    logging:
      driver: splunk
      options:
        splunk-url: http://victorialogs:9428
        splunk-token: any-token
```

## Time field

VictoriaLogs uses the `time` field as [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field)
for the logs ingested via Splunk endpoint. Other field can be used instead by setting `-splunk.timeField` command-line flag.

See also [HTTP query string parameters](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-query-string-parameters).

## Message field

By default VictoriaLogs uses the first non-empty field from the following list as [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):
for the logs ingested via Splunk endpoint:
- `event`
- `event.log`
- `event.line`
- `event.message`

Other fields can be used instead by passing a comma-separated list to `-splunk.msgField` command-line flag.

See also [HTTP query string parameters](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-query-string-parameters).

## Stream fields

VictoriaLogs uses `host`, `source` and `sourcetype` fields as [stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields)
for logs ingested via Splunk protocol. The list of log stream fields can be changed via `-splunk.streamFields` command-line flag
by providing comma-separated list of fields.

See also [HTTP query string parameters](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-query-string-parameters).

## Dropping fields

VictoriaLogs can be configured for skipping the given [log fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model)
for logs ingested via Splunk protocol, by using `-splunk.ignoreFields` command-line flag. This flag accepts comma-separated list of log fields to ignore.
This list can contain log field prefixes ending with `*` such as `some-prefix*`. In this case all the fields starting from `some-prefix` are ignored.

See also [HTTP query string parameters](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-query-string-parameters).

## Multitenancy

By default VictoriaLogs stores logs ingested via Splunk protocol into `(AccountID=0, ProjectID=0)` [tenant](https://docs.victoriametrics.com/victorialogs/#multitenancy).
This can be changed by passing the needed tenant in the format `AccountID:ProjectID` at the `-splunk.tenantID` command-line flag.
For example, `-splunk.tenantID=123:456` would store logs into `(AccountID=123, ProjectID=456)` tenant.

See also:

- [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
- [HTTP query string parameters](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-query-string-parameters).
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).
- [Docker-compose demo for Splunk integration with VictoriaLogs](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/vector/splunk).

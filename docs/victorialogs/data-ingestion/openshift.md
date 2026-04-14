---
weight: 4
title: OpenShift
disableToc: true
menu:
  docs:
    parent: "victorialogs-data-ingestion"
    weight: 12
tags:
  - logs
---
VictoriaLogs supports two options for ingestion logs using OpenShift native cluster logging operator:
* using `elasticsearch` output (starting [v6.4.2](https://docs.redhat.com/en/documentation/red_hat_openshift_logging/6.4/html/release_notes/logging-release-notes#openshift-logging-release-notes-6-4-2-enhancements_logging-release-notes)).
* using `http` output (starting [v6.5.0](https://docs.redhat.com/en/documentation/red_hat_openshift_logging/6.5/html/release_notes/logging-release-notes#openshift-logging-release-notes-6-5-0-enhancements_logging-release-notes)).

## Elasticsearch output

Starting cluster logging operator v6.4.2 `elasticsearch` output supports `headers` option, that allows to pass custom HTTP headers.

```yaml
apiVersion: logging.openshift.io/v1
kind: ClusterLogForwarder
metadata:
  name: instance
  namespace: openshift-logging
spec:
  outputs:
    - name: vlogs
      type: elasticsearch
      elasticsearch:
        url: https://victorialogs.example.com/insert/elasticsearch
        headers:
          VL-Msg-Field: "msg,_msg,message"
# pipelines are expected to be configured there
```

## HTTP output

Starting cluster logging operator v6.5.0 `http` output supports `encoding` option, that should be set to `ndjson` to make log forwarder sending newline delimited data to HTTP output

```yaml
apiVersion: logging.openshift.io/v1
kind: ClusterLogForwarder
metadata:
  name: instance
  namespace: openshift-logging
spec:
  outputs:
    - name: vlogs
      type: http
      http:
        url: https://victorialogs.example.com/insert/jsonline
        encoding: ndjson
        headers:
          VL-Msg-Field: "msg,_msg,message"
# pipelines are expected to be configured there
```

VictoriaLogs supports various HTTP headers, which can be used during data ingestion - see the list [here](https://docs.victoriametrics.com/victorialogs/data-ingestion/#http-headers).

See also:

* [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
* [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).

---
weight: 13
title: KENT (Kubernetes events)
disableToc: true
menu:
  docs:
    parent: "victorialogs-data-ingestion"
    weight: 13
tags:
  - logs
  - kubernetes
---
[KENT](https://github.com/lev-stas/KENT) (Kubernetes Events Notifier) is a third-party exporter, which watches Kubernetes events
(the same data as `kubectl get events` shows) and ships them to VictoriaLogs
via the [JSON stream API](https://docs.victoriametrics.com/victorialogs/data-ingestion/#json-stream-api).

Install KENT with its Helm chart:

```sh
git clone https://github.com/lev-stas/KENT.git
cd KENT/deploy/chart
helm upgrade --install -n monitoring -f values.yaml kent .
```

Point it at VictoriaLogs in `values.yaml`:

```yaml
config:
  victorialogs:
    enabled: true
    endpoint: "http://victorialogs:9428"
    clusterID: "prod"
    streamFields: ["k8s.namespace"]
```

Every event becomes a structured log entry with `k8s.*` and `event.*` fields. The `clusterID` field together with the fields
listed in `streamFields` is registered as [log stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields),
so per-cluster and per-namespace queries stay fast. [Multitenancy](https://docs.victoriametrics.com/victorialogs/#multitenancy)
is supported via `accountID` and `projectID` options.

Events re-delivered by the Kubernetes watch are deduplicated and failed sends are retried with backoff,
so VictoriaLogs restarts don't lead to duplicated or lost events.

See [KENT documentation](https://github.com/lev-stas/KENT#readme) for authentication, TLS and other configuration options.

See also:

- [Data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).
- [How to query VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/).

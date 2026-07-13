---
weight: 102
title: Roadmap
disableToc: true
menu:
  docs:
    parent: "victorialogs"
    weight: 102
    title: Roadmap
tags:
  - logs
aliases:
- /victorialogs/Roadmap.html
- /VictoriaLogs/Roadmap.html
---

- [ ] Store data on object storage such as S3, GCS or Minio (issue [#48](https://github.com/VictoriaMetrics/VictoriaLogs/issues/48), PR [#1155](https://github.com/VictoriaMetrics/VictoriaLogs/pull/1155)).
- [ ] Log transformations in `vlagent` (issue [#858](https://github.com/VictoriaMetrics/VictoriaLogs/issues/858), PR [#1508](https://github.com/VictoriaMetrics/VictoriaLogs/pull/1508)).
- [ ] Migration tooling from other logging systems, similar to [vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/) (issue [#521](https://github.com/VictoriaMetrics/VictoriaLogs/issues/521)).
- [ ] Kafka support in `vlagent` (Enterprise, issue [#885](https://github.com/VictoriaMetrics/VictoriaLogs/issues/885)).
- [ ] Per-tenant statistics, for example for internal billing (Enterprise, issue [#65](https://github.com/VictoriaMetrics/VictoriaLogs/issues/65)).
- [ ] Per-tenant quotas on ingest rate, stream cardinality and query cost (Enterprise, issue [#9](https://github.com/VictoriaMetrics/VictoriaLogs/issues/9)).
- [ ] Backup, restore and backup manager tooling, on top of the existing storage snapshots (issue [#123](https://github.com/VictoriaMetrics/VictoriaLogs/issues/123)).
- [ ] Multitenant querying: an endpoint that reads across tenants and returns per-row tenant fields (issue [#91](https://github.com/VictoriaMetrics/VictoriaLogs/issues/91), PR [#1576](https://github.com/VictoriaMetrics/VictoriaLogs/pull/1576)).
- [ ] Asynchronous queries for long-running reports, which can be submitted, polled and fetched without holding the connection open (issue [#1398](https://github.com/VictoriaMetrics/VictoriaLogs/issues/1398)).
- [ ] Ingestion rate limiting by throughput (logs/sec, MB/sec), beyond the existing `-maxConcurrentInserts` concurrency limit (issue [#887](https://github.com/VictoriaMetrics/VictoriaLogs/issues/887)).

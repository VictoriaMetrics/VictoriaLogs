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

- [ ] Ability to store data to object storage (such as S3, GCS, Minio).
- [ ] Native support for ingesting logs via the Splunk HTTP Event Collector (HEC) into VictoriaLogs (PR [#710](https://github.com/VictoriaMetrics/VictoriaLogs/pull/710)).
- [ ] Support for log transformations in vlagent (issue [#858](https://github.com/VictoriaMetrics/VictoriaLogs/issues/858)).
- [ ] Migration tooling from other logging systems to VictoriaLogs (similar to [vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/)).
- [ ] Kafka support in vlagent (issue [#885](https://github.com/VictoriaMetrics/VictoriaLogs/issues/885)).
- [ ] Per-tenant stats (issue [#65](https://github.com/VictoriaMetrics/VictoriaLogs/issues/65)) for quotas (ingest rate, stream cardinality, query cost).
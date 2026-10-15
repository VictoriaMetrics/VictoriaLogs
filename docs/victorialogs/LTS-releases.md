---
weight: 103
title: Long-term support releases
description: "Long-term support release lines of VictoriaLogs for enterprise customers."
menu:
  docs:
    parent: "victorialogs"
    identifier: "victorialogs-lts-releases"
    weight: 103
    title: Long-term support releases
tags:
  - logs
  - enterprise
--- 

[Enterprise version of VictoriaLogs](https://docs.victoriametrics.com/victoriametrics/enterprise/) provides long-term support lines of releases (aka LTS releases).
Every LTS line receives bugfixes and [security fixes](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/SECURITY.md) for 12 months after
the initial release. New LTS lines are published every 6 months, so the latest two LTS lines are supported at any given moment. This gives up to 6 months
for the migration to new LTS lines for [VictoriaLogs Enterprise](https://docs.victoriametrics.com/victoriametrics/enterprise/) users.

LTS releases are published for [Enterprise versions of VictoriaLogs](https://docs.victoriametrics.com/victoriametrics/enterprise/) only.
When a new LTS line is created, the new LTS release might be publicly available for everyone until the new major OS release will be published.

All the bugfixes and security fixes, which are included in LTS releases, are also available in [the latest release](https://github.com/VictoriaMetrics/VictoriaLogs/releases/latest),
so non-enterprise users are advised to regularly [upgrade](https://docs.victoriametrics.com/victorialogs/#upgrading) VictoriaLogs components
to [the latest available releases](https://docs.victoriametrics.com/victorialogs/changelog/).

## Currently supported LTS release lines

- v1.52.x - the latest one is [v1.52.1 LTS release](https://github.com/VictoriaMetrics/VictoriaLogs/releases/tag/v1.52.1)

---
build:
  list: never
  publishResources: false
  render: never
sitemap:
  disable: true
---
VictoriaLogs is [open source](https://github.com/VictoriaMetrics/VictoriaLogs/) user-friendly database for logs
from [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics/).

## Features

VictoriaLogs provides the following features:

- It is resource-efficient and fast. It uses up to 30x less RAM and up to 15x less disk space than other solutions such as Elasticsearch and Grafana Loki.
  See [these benchmarks](https://docs.victoriametrics.com/victorialogs/#benchmarks) and [this article](https://itnext.io/how-do-open-source-solutions-for-logs-work-elasticsearch-loki-and-victorialogs-9f7097ecbc2f) for details.
  See also [the post from a happy user, who replaced 27-node Elasticsearch with a single-node VictoriaLogs](https://aus.social/@phs/114583927679254536),
  [this post from happy users, who replaced Loki with VictoriaLogs](https://www.truefoundry.com/blog/victorialogs-vs-loki)
  and [this post from a happy user who replaced grep with VictoriaLogs](https://chronicles.mad-scientist.club/tales/grepping-logs-remains-terrible/).
- VictoriaLogs' capacity and performance scales linearly with the available resources (CPU, RAM, disk IO, disk space).
  It runs smoothly on Raspberry PI and on servers with hundreds of CPU cores and terabytes of RAM.
  It can scale horizontally to hundreds of nodes in [cluster mode](https://docs.victoriametrics.com/victorialogs/cluster/).
- It can accept logs from popular log collectors. See [these docs](https://docs.victoriametrics.com/victorialogs/data-ingestion/).
- It is much easier to set up and operate compared to Elasticsearch and Grafana Loki, since it is a single zero-config executable.
  See [these docs](https://docs.victoriametrics.com/victorialogs/quickstart/).
- It provides easy yet powerful query language, which supports fast full-text search, fast advanced analytics and fast data extraction and transformation at query time.
  See [LogsQL docs](https://docs.victoriametrics.com/victorialogs/logsql/).
- It provides [built-in web UI](https://docs.victoriametrics.com/victorialogs/querying/#web-ui) for logs' exploration.
- It provides [Grafana plugin](https://docs.victoriametrics.com/victorialogs/integrations/grafana/) for building arbitrary dashboards in Grafana.
- It provides [interactive command-line tool for querying VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/).
- It can be seamlessly combined with good old Unix tools for log analysis such as `grep`, `less`, `sort`, `jq`, etc.
  See [these docs](https://docs.victoriametrics.com/victorialogs/querying/#command-line) for details.
- It support [log fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model) with high cardinality (e.g. high number of unique values) such as `trace_id`, `user_id` and `ip`.
- It is optimized for logs with hundreds of fields (aka [`wide events`](https://jeremymorrell.dev/blog/a-practitioners-guide-to-wide-events/)).
- It supports multitenancy - see [these docs](https://docs.victoriametrics.com/victorialogs/#multitenancy).
- It supports out-of-order logs' ingestion aka backfilling.
- It supports live tailing for newly ingested logs. See [these docs](https://docs.victoriametrics.com/victorialogs/querying/#live-tailing).
- It supports selecting surrounding logs in front and after the selected logs. See [these docs](https://docs.victoriametrics.com/victorialogs/logsql/#stream_context-pipe).
- It supports alerting - see [these docs](https://docs.victoriametrics.com/victorialogs/vmalert/).
- It fits well [RUM](https://en.wikipedia.org/wiki/Real_user_monitoring) and [SIEM](https://en.wikipedia.org/wiki/Security_information_and_event_management) use cases.
  It automatically parses [CEF messages sent over Syslog protocol](https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#cef).

See also [articles about VictoriaLogs](https://docs.victoriametrics.com/victorialogs/articles/).

If you have questions about VictoriaLogs, then read [this FAQ](https://docs.victoriametrics.com/victorialogs/faq/).
Also feel free asking any questions at [VictoriaMetrics community Slack chat](https://victoriametrics.slack.com/),
you can join it via [Slack Inviter](https://slack.victoriametrics.com/).

See [quick start docs](https://docs.victoriametrics.com/victorialogs/quickstart/) for start working with VictoriaLogs.

If you want playing with VictoriaLogs web UI and [LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/) query language,
then go to [VictoriaLogs demo playground](https://play-vmlogs.victoriametrics.com/) and
to [Grafana plugin playground for VictoriaLogs](https://play-grafana.victoriametrics.com/d/be5zidev72m80f/k8s-logs-via-victorialogs).

## Tuning

- No need in tuning for VictoriaLogs - it uses reasonable defaults for command-line flags, which are automatically adjusted for the available CPU and RAM resources.
- No need in tuning for Operating System - VictoriaLogs is optimized for default OS settings.
  The only option is increasing the limit on [the number of open files in the OS](https://medium.com/@muhammadtriwibowo/set-permanently-ulimit-n-open-files-in-ubuntu-4d61064429a).
- The recommended filesystem is `ext4`, the recommended persistent storage is [persistent HDD-based disk on GCP](https://cloud.google.com/compute/docs/disks/#pdspecs),
  since it is protected from hardware failures via internal replication and it can be [resized on the fly](https://cloud.google.com/compute/docs/disks/add-persistent-disk#resize_pd).
  If you plan to store more than 1TB of data on `ext4` partition, then the following options are recommended to pass to `mkfs.ext4`:

  ```sh
  mkfs.ext4 ... -O 64bit,huge_file,extent -T huge
  ```

## Monitoring

VictoriaLogs exposes internal metrics in Prometheus exposition format at `http://localhost:9428/metrics` page.
It is recommended to set up monitoring of these metrics via VictoriaMetrics
(see [these docs](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#how-to-scrape-prometheus-exporters-such-as-node-exporter)),
vmagent (see [these docs](https://docs.victoriametrics.com/victoriametrics/vmagent/#how-to-collect-metrics-in-prometheus-format)) or via Prometheus.

See [metrics reference](https://docs.victoriametrics.com/victorialogs/metrics/) for a comprehensive list of all available metrics with detailed descriptions.

We recommend installing Grafana dashboard for [VictoriaLogs single-node](https://grafana.com/grafana/dashboards/22084) or [cluster](https://grafana.com/grafana/dashboards/23274).

We recommend setting up [alerts](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/deployment/docker/rules/alerts-vlogs.yml)
via [vmalert](https://docs.victoriametrics.com/victoriametrics/vmalert/) or via Prometheus.

VictoriaLogs emits its own logs to stdout. It is recommended to investigate these logs during troubleshooting.

## Upgrading

It is safe upgrading VictoriaLogs to new versions unless [release notes](https://docs.victoriametrics.com/victorialogs/changelog/) say otherwise.
It is safe to skip multiple versions during the upgrade unless [release notes](https://docs.victoriametrics.com/victorialogs/changelog/) say otherwise.
It is recommended to perform regular upgrades to the latest version, since it may contain important bug fixes, performance optimizations or new features.

It is also safe to downgrade to older versions unless [release notes](https://docs.victoriametrics.com/victorialogs/changelog/) say otherwise.

The following steps must be performed during the upgrade / downgrade procedure:

- Send `SIGINT` signal to VictoriaLogs process in order to gracefully stop it.
  See [how to send signals to processes](https://stackoverflow.com/questions/33239959/send-signal-to-process-from-command-line).
- Wait until the process stops. This can take a few seconds.
- Start the upgraded VictoriaLogs.

## Retention

By default, VictoriaLogs stores log entries with timestamps in the time range `[now-7d, now]`, while dropping logs outside the given time range.
E.g. it uses the retention of 7 days. The retention can be configured with `-retentionPeriod` command-line flag.
This flag accepts values starting from `1d` (one day) up to `100y` (100 years). See [these docs](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations)
for the supported duration formats.

For example, the following command starts VictoriaLogs with the retention of 8 weeks:

```sh
/path/to/victoria-logs -retentionPeriod=8w
```

See also [retention by disk space usage](https://docs.victoriametrics.com/victorialogs/#retention-by-disk-space-usage).

VictoriaLogs stores the [ingested](https://docs.victoriametrics.com/victorialogs/data-ingestion/) logs in per-day partition directories.
It automatically drops partition directories outside the configured retention.

VictoriaLogs automatically drops logs at [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/) stage
if they have timestamps outside the configured retention. A sample of dropped logs is logged with `WARN` message in order to simplify troubleshooting.
The `vl_rows_dropped_total` [metric](https://docs.victoriametrics.com/victorialogs/metrics/#vl_rows_dropped_total) is incremented each time an ingested log entry is dropped because of timestamp outside the retention.
It is recommended to set up the following alerting rule at [vmalert](https://docs.victoriametrics.com/victoriametrics/vmalert/) in order to be notified
when logs with wrong timestamps are ingested into VictoriaLogs:

```metricsql
rate(vl_rows_dropped_total[5m]) > 0
```

By default, VictoriaLogs doesn't accept log entries with timestamps bigger than `now+2d`, e.g. 2 days in the future.
If you need accepting logs with bigger timestamps, then specify the desired "future retention" via `-futureRetention` command-line flag.
This flag accepts values starting from `1d`. See [these docs](https://prometheus.io/docs/prometheus/latest/querying/basics/#time-durations)
for the supported duration formats.

For example, the following command starts VictoriaLogs, which accepts logs with timestamps up to a year in the future:

```sh
/path/to/victoria-logs -futureRetention=1y
```

## Retention by disk space usage

VictoriaLogs can be configured to automatically drop older per-day partitions based on disk space usage using one of two approaches:

### Absolute disk space limit

Use the `-retention.maxDiskSpaceUsageBytes` command-line flag to set a fixed threshold. VictoriaLogs will drop old per-day partitions
if the total size of data at [`-storageDataPath` directory](https://docs.victoriametrics.com/victorialogs/#storage) becomes bigger than the specified limit.
For example, the following command starts VictoriaLogs, which drops old per-day partitions if the total [storage](https://docs.victoriametrics.com/victorialogs/#storage) size becomes bigger than `100GiB`:

```sh
/path/to/victoria-logs -retention.maxDiskSpaceUsageBytes=100GiB
```

### Percentage-based disk space limit

Use the `-retention.maxDiskUsagePercent` command-line flag to set a dynamic threshold based on the filesystem's total capacity.
VictoriaLogs will drop old per-day partitions if the filesystem containing the [`-storageDataPath` directory](https://docs.victoriametrics.com/victorialogs/#storage) exceeds the specified percentage usage.
For example, the following command starts VictoriaLogs, which drops old per-day partitions if the filesystem usage exceeds 80%:

```sh
/path/to/victoria-logs -retention.maxDiskUsagePercent=80
```

This approach is particularly useful in environments where the total disk capacity may vary (e.g., cloud environments with resizable volumes)
or when you want to maintain a consistent percentage of free space regardless of the total disk size.

**Important:** The `-retention.maxDiskSpaceUsageBytes` and `-retention.maxDiskUsagePercent` flags are mutually exclusive.
VictoriaLogs will refuse to start if both flags are set simultaneously.

VictoriaLogs usually compresses logs by 10x or more times. This means that VictoriaLogs can store more than a terabyte of uncompressed
logs when it runs with `-retention.maxDiskSpaceUsageBytes=100GiB` or when using percentage-based retention on a large filesystem.

VictoriaLogs keeps at least two last days of data in order to guarantee that the logs for the last day can be returned in queries.
This means that the total disk space usage may exceed the configured threshold if the size of the last two days of data
exceeds the limit.

The [`-retentionPeriod`](https://docs.victoriametrics.com/victorialogs/#retention) is applied independently to the disk space usage limits. This means that
VictoriaLogs automatically drops logs older than 7 days by default if only a disk space usage flag is set.
Set the `-retentionPeriod` to some big value (e.g. `100y` - 100 years) if logs shouldn't be dropped because of time-based retention.
For example:

```sh
/path/to/victoria-logs -retention.maxDiskSpaceUsageBytes=10TiB -retentionPeriod=100y
```

or

```sh
/path/to/victoria-logs -retention.maxDiskUsagePercent=85 -retentionPeriod=100y
```

## Backfilling

VictoriaLogs accepts logs with timestamps in the time range `[now-retentionPeriod ... now+futureRetention]`,
where `retentionPeriod` is the value for the `-retentionPeriod` command-line flag and `futureRetention` is the value for the `-futureRetention` command-line flag.
Sometimes it is needed to reject logs older than the given age. This can be achieved by passing `-maxBackfillAge=d` command-line flag to VictoriaLogs,
where `d` is the maximum age of logs to be accepted. Older logs are rejected and a sample of these logs is put into VictoriaLogs output logs, so they could be investigated.
For example, the following command starts VictoriaLogs, which rejects logs older than 1 hour:

```sh
/path/to/victoria-logs -maxBackfillAge=1h
```

## Storage

VictoriaLogs stores all its data in a single directory - `victoria-logs-data`. The path to the directory can be changed via `-storageDataPath` command-line flag.
For example, the following command starts VictoriaLogs, which stores the data at `/var/lib/victoria-logs`:

```sh
/path/to/victoria-logs -storageDataPath=/var/lib/victoria-logs
```

VictoriaLogs automatically creates the `-storageDataPath` directory on the first run if it is missing. VictoriaLogs stores logs
per every day into a spearate subdirectory (aka per-day partition). See [partitions lifecycle](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle) for details.

VictoriaLogs switches to cluster mode if `-storageNode` command-line flag is specified:

- It stops storing the ingested logs locally in cluster mode. It spreads them evenly among `vlstorage` nodes specified via the `-storageNode` command-line flag.
- It stops querying the locally stored logs in cluster mode. It queries `vlstorage` nodes specified via `-storageNode` command-line flag.

See [cluster mode docs](https://docs.victoriametrics.com/victorialogs/cluster/) for details.

## Partitions lifecycle

The ingested logs are stored in per-day subdirectories (partitions) at the `<-storageDataPath>/partitions/` directory. The per-day subdirectories have `YYYYMMDD` names.
For example, the directory with the name `20250418` contains logs with [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field) values
at April 18, 2025 UTC. This allows flexible data management.

For example, old per-day data is automatically and quickly deleted according to the provided [retention policy](https://docs.victoriametrics.com/victorialogs/#retention) by removing the corresponding per-day subdirectory (partition).

VictoriaLogs supports the following HTTP API endpoints at `victoria-logs:9428` address for managing partitions:

- `/internal/partition/attach?name=YYYYMMDD` - attaches the partition directory with the given name `YYYYMMDD` to VictoriaLogs,
  so it becomes visible for querying and can be used for data ingestion.
  The directory must be placed inside `<-storageDataPath>/partitions` and it must contain valid data for the given `YYYYMMDD` day.
- `/internal/partition/detach?name=YYYYMMDD` - detaches the partition directory with the given name `YYYYMMDD` from VictoriaLogs,
  so it is no longer visible for querying and cannot be used for data ingestion.
  The `/internal/partition/detach` endpoint waits until all the concurrently executed queries stop reading the data from the detached partition
  before returning. This allows safe on-disk manipulions of the detached partitions by external tools after returning from the `/internal/partition/detach` endpoint.
  Detached partitions are automatically attached after VictoriaLogs restart if the corresponding subdirectories at `<-storageDataPath>/partitions/` aren't removed.
- `/internal/partition/list` - returns JSON-encoded list of currently active partitions, which can be passed to `/internal/partition/detach` endpoint via `name` query arg.
- `/internal/partition/snapshot/create?partition_prefix=<prefix>` - creates [snapshots](https://medium.com/@valyala/how-victoriametrics-makes-instant-snapshots-for-multi-terabyte-time-series-data-e1f3fb0e0282)
  for partitions matching the given `<prefix>`. The `<prefix>` must match the following formats:
  - `YYYYMMDD` - then the snapshot for the given per-day partition is created.
  - `YYYYMM` - then snapshots for per-day partitions at the given month are created.
  - `YYYY` - then snapshots for per-day partitions at the given year are created.
  If the `partition_prefix` arg is missing then snapshots for all the active per-day partitions are created.
  The endpoint returns JSON-encoded array with paths to created snapshots. It is safe to make backups from
  the created snapshots according to [these instructions](https://docs.victoriametrics.com/victorialogs/#backup-and-restore).
  Snapshots can be removed via `/internal/partition/snapshot/delete?path=<snapshot-path>`.
  It is recommended removing unneeded snapshots on a regular basis in order to free up storage space occupied by these snapshots.
- `/internal/partition/snapshot/list` - returns JSON-encoded list of paths to per-day partition snapshots created via `/internal/partition/snapshot/create`.
- `/internal/partition/snapshot/delete?path=<snapshot-path>` - deletes a snapshot created via `/internal/partition/snapshot/create`.
  The `<snapshot-path>` can be taken from the output of `/internal/partition/snapshot/list`.

These endpoints can be protected from unauthorized access via `-partitionManageAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

These endpoints can be used also for setting up automated multi-tier storage schemes where recently ingested logs are stored to VictoriaLogs instances
with fast NVMe (SSD) disks, while historical logs are gradully migrated to VictoriaLogs instances with slower, but bigger and less expensive HDD disks.
This scheme can be implemented with the following simple cron job, which must run once per day:

1. To make a snapshot for the older day stored at NVMe via `/internal/partition/snapshot/create?partition_prefix=YYYYMMDD` endpoint.
1. To copy the snapshot to the `<-storageDataPath>/partitions/YYYYMMDD` directory at VictoriaMetrics with HDD via [`rsync`](https://en.wikipedia.org/wiki/Rsync).
1. To detach the copied partition from the VictoriaLogs with NVMe via `/internal/partition/detach?name=YYYYMMDD` endpoint.
1. To attach the copied partition to the VictoriaLogs with HDD via `/internal/partition/attach?name=YYYYMMDD` endpoint.
1. To delete the copied partition directory from the VictoriaLogs with NVMe via `rm -rf <-storageDataPath>/partitions/YYYYMMDD` command.

All the VictoriaLogs instances with NVMe and HDD disks can be queried simultaneously via `vlselect` component of [VictoriaLogs cluster](https://docs.victoriametrics.com/victorialogs/cluster/),
since [single-node VictoriaLogs instances can be a part of cluster](https://docs.victoriametrics.com/victorialogs/cluster/#single-node-and-cluster-mode-duality).

## Capacity planning

It is recommended leaving the following amounts of spare resource for smooth work of VictoriaLogs:

- 50% of free RAM for reducing the probability of OOM (out of memory) crashes and slowdowns during temporary spikes in workload.
- 50% of spare CPU for reducing the probability of slowdowns during temporary spikes in workload.
- At least 20% of free storage space at the directory pointed by the [`-storageDataPath`](https://docs.victoriametrics.com/victorialogs/#storage) command-line flag.
  Too small amounts of free disk space may result in significant slowdown for both data ingestion and querying
  because of inability to merge newly created smaller data parts into bigger data parts.

## Logging new streams

VictoriaLogs can log new [log streams](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) during [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/).
This is useful during the debugging of high cardinality or churn rate issues for the ingested log streams.
This functionality can be enabled either on a permanent basis via `-logNewStreams` command-line flag or temporarily for the given number of seconds
by sending HTTP request to `http://victoria-logs:9428/internal/log_new_streams?seconds=secs`. For example, the following command enables temporary logging
of new log streams for 10 seconds:

```
curl http://victoria-logs:9428/internal/log_new_streams?seconds=10
```

See also [data ingestion troubleshooting](https://docs.victoriametrics.com/victorialogs/data-ingestion/#troubleshooting).

## Forced merge

VictoriaLogs performs data compactions in background in order to keep good performance characteristics when accepting new data.
These compactions (merges) are performed independently on per-day partitions.
This means that compactions are stopped for per-day partitions if no new data is ingested into these partitions.
Sometimes it is necessary to trigger compactions for old partitions. In this case forced compaction may be initiated on the specified per-day partition
by sending request to `/internal/force_merge?partition_prefix=YYYYMMDD`,
where `YYYYMMDD` is per-day partition name. For example, `http://victoria-logs:9428/internal/force_merge?partition_prefix=20240921` would initiate forced
merge for September 21, 2024 partition. The call to `/internal/force_merge` returns immediately, while the corresponding forced merge continues running in background.

Forced merges may require additional CPU, disk IO and storage space resources. It is unnecessary to run forced merge under normal conditions,
since VictoriaLogs automatically performs optimal merges in background when new data is ingested into it.

The `/internal/force_merge` endpoint can be protected from unauthorized access via `-forceMergeAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

## Forced flush

VictoriaLogs puts the recently [ingested logs](https://docs.victoriametrics.com/victorialogs/data-ingestion/) into in-memory buffers,
which aren't available for [querying](https://docs.victoriametrics.com/victorialogs/querying/) for up to a second.
If you need querying logs immediately after their ingestion, then the `/internal/force_flush` HTTP endpoint must be requested
before querying. This endpoint converts in-memory buffers with the recently ingested logs into searchable data blocks.

It isn't recommended requesting the `/internal/force_flush` HTTP endpoint on a regular basis, since this increases CPU usage
and slows down data ingestion. It is expected that the `/internal/force_flush` is requested in automated tests, which need querying
the recently ingested data.

The `/internal/force_flush` endpoint can be protected from unauthorized access via `-forceFlushAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

## How to delete logs

By default VictoriaLogs doesn't allow deleting the [ingested logs](https://docs.victoriametrics.com/victorialogs/data-ingestion/).
This is good from security PoV - an attacker cannot remove the existing logs. There are cases when it is needed to delete logs
because of [GDPR compliance](https://en.wikipedia.org/wiki/General_Data_Protection_Regulation) or because some sensitive info is ingested in the logs.
VictoriaLogs enables HTTP API for deleting logs if `-delete.enable` command-line flag is passed to it.
The following HTTP endpoints are exposed at `http://victoria-logs:9428/` in this case:

- `/delete/run_task?filter=<logsql_filter>` - starts an asynchronous task for deletion of the logs matching the given `<logsql_filter>`.
  The `<logsql_filter>` may contain arbitrary [LogsQL filter](https://docs.victoriametrics.com/victorialogs/logsql/#filters).
  For example, request to `http://victoria-logs:9428/delete/run_task?filter={app=nginx}` starts a task for deleting all the logs with
  `{app="nginx"}` [log stream field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields).
  When calling this endpoint via `curl`, make sure to URL-encode the `{...}` filter (aka [percent-encoding](https://en.wikipedia.org/wiki/Percent-encoding)), otherwise `curl` may strip the curly braces and the filter will fail to parse. For example, `{app=nginx}` becomes `%7Bapp%3Dnginx%7D`, so the full request is:

  ```bash
  curl 'http://victoria-logs:9428/delete/run_task?filter=%7Bapp%3Dnginx%7D'
  ```

  This endpoint returns `{"task_id":"<id>"}` response, where `<id>` is an unique id of the deletion task, which can be used
  for tracking the status of the deletion operation and for canceling the deletion task.
  The deletion operation may take significant amounts of time when VictoriaLogs contains terabytes of logs, since the deletion operation
  rewrites all the stored logs. That's why it isn't recommended to delete logs on a frequent basis - it is intended for rare exceptional cases
  such as GDPR compliance or removal of accidentally written security-sensitive data.

- `/delete/stop_task?task_id=<id>` - cancels the deletion task with the given `<id>`. If the canceled task was already running,
  then it doesn't restore already deleted data.

- `/delete/active_tasks` - returns a JSON array with the following information about active deletion tasks:
  - `task_id` - the `id` of the task
  - `tenant_ids` - the list of [tenants](https://docs.victoriametrics.com/victorialogs/#multitenancy) for the given deletion task
  - `filter` - the [LogsQL filter](https://docs.victoriametrics.com/victorialogs/logsql/#filters) passed to `/delete/run_task?filter=...`.
  - `start_time` - the start time of the deletion task.

The logs scheduled for the deletion via `/delete/run_task` endpoint main remain visible until the deletion task is complete.
The deletion task is complete when the `/delete/active_task` endpoint stops returning it.

If the deletion API must be enabled in [cluster version of VictoriaLogs](https://docs.victoriametrics.com/victorialogs/cluster/),
then `-delete.enable` command-line flag must be passed to `vlselect` nodes (this enables the deletion API at `vlselect` nodes),
while `-internaldelete.enable` command-line flag must be passed to `vlstorage` nodes (this enables internal cluster API
for receiving deletion requests from `vlselect` nodes).

## High Availability

### High Availability (HA) Setup with VictoriaLogs Single-Node Instances

The setup consists of the following components:

- **Log Collector**: The log collector should support sending the same collected data to multiple destinations (aka replication).
It is recommended to use [vlagent](https://docs.victoriametrics.com/victorialogs/vlagent/). Other popular log collectors also provide this ability:
- [How to setup replication at FluentBit](https://docs.fluentbit.io/manual/concepts/data-pipeline/router)
- [How to setup replication at Logstash](https://www.elastic.co/guide/en/logstash/current/output-plugins.html)
- [How to setup replication at Fluentd](https://docs.fluentd.org/output/copy)
- [How to setup replication at Vector](https://vector.dev/docs/reference/configuration/sinks/)

- **VictoriaLogs Single-Node Instances**: send copies of the collected logs to multiple instances of VictoriaLogs in distinct availability zones to achieve HA.

- **[vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/#load-balancing)**: query logs via `vmauth` - it balances incoming queries among available VictoriaLogs instances,
  and automatically re-routes requests to healthy backends if some of the instances are temporarily unavailable.

![VictoriaLogs Single-Node Instance High-Availability schema](ha-victorialogs-single-node.webp)

Here are the working examples of HA configuration for VictoriaLogs using Docker Compose:

- [Fluent Bit](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/fluentbit/)
- [Logstash](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/logstash/)
- [Vector](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/docker/victorialogs/vector/)

## Backup and restore

VictoriaLogs stores data into independent per-day partitions. Every partition is stored in a separate directory - `<-storageDataPath>/partitions/YYYYMMDD`.

The following steps must be performed to make a backup of the given `YYYYMMDD` partition:

1. To create a snapshot for the given per-day partition via `/internal/partition/snapshot/create?partition_prefix=YYYYMMDD` HTTP endpoint
   (see [partitions lifecycle](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle) docs).
   This endpoint returns a path to the created snapshot - `<path-to-snapshot>`.

1. To backup the created snapshot with [`rsync`](https://en.wikipedia.org/wiki/Rsync):

   ```sh
   rsync -avh --progress --delete <path-to-snapshot> <username>@<host>:<path-to-backup>/YYYYMMDD
   ```

   The `--delete` option is required in the command above in order to ensures that the backup contains the full copy of the original data without superfluous files.

1. To remove the snapshot with `/internal/partition/snapshot/delete?path=<path-to-snapshot>` endpoint. It is important to remove unneeded snapshots in order to free up storage space.

The following steps must be performed for restoring the partition data from backup:

1. To stop VictoriaLogs instance or to detach the `YYYYMMDD` partition, which is going to be restored from backup,
   from the running VictoriaLogs via `/internal/partition/detach?name=YYYYMMDD` HTTP endpoint according to [these docs](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle).

1. To copy the partition from backup with `rsync`:

   ```sh
   rsync -avh --progress --delete <username>@<host>:<path-to-backup>/YYYYMMDD <-storageDataPath>/partitions/
   ```

   The `--delete` option is required in the command above in order to ensure that the partition contains the full copy of the backup without superfluous files.

1. To start VictoriaLogs instance or to attach the restored partition to the running VictoriaLogs instance via `/internal/partition/attach?name=YYYYMMDD` HTTP endpoint
   according to [these docs](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle).

It is also possible to use **the disk snapshot** feature provided by the operating system or cloud provider in order to perform a backup.

## Multitenancy

VictoriaLogs supports multitenancy. A tenant is identified by `(AccountID, ProjectID)` pair, where `AccountID` and `ProjectID` are arbitrary 32-bit unsigned integers.
The `AccountID` and `ProjectID` can be set during [data ingestion](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
and [querying](https://docs.victoriametrics.com/victorialogs/querying/) via `AccountID` and `ProjectID` request headers.

If `AccountID` and/or `ProjectID` request headers aren't set, then the default `0` value is used.

VictoriaLogs has very low overhead for per-tenant management, so it is OK to have thousands of tenants in a single VictoriaLogs instance.

VictoriaLogs doesn't perform per-tenant authorization. Use [vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/) or similar tools for per-tenant authorization.
See [Security and Load balancing docs](https://docs.victoriametrics.com/victorialogs/security-and-lb/) for details.

### Multitenancy access control

Enforce access control for tenants by using [vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/). Access control can be configured for each tenant by setting up the following rules:

```yaml
users:
  - username: "foo"
    password: "bar"
    url_map:
      - src_paths:
        - "/select/.*"
        - "/insert/.*"
        headers:
          - "AccountID: 1"
          - "ProjectID: 0"
        url_prefix:
          - "http://localhost:9428/"

  - username: "baz"
    password: "bar"
    url_map:
      - src_paths: ["/select/.*"]
        headers:
          - "AccountID: 2"
          - "ProjectID: 0"
        url_prefix:
          - "http://localhost:9428/"
```

This configuration allows `foo` to use the `/select/.*` and `/insert/.*` endpoints with `AccountID: 1` and `ProjectID: 0`, while `baz` can only use the `/select/.*` endpoint with `AccountID: 2` and `ProjectID: 0`.

See also [Security and Load balancing docs](https://docs.victoriametrics.com/victorialogs/security-and-lb/).

## Security

It is expected that VictoriaLogs runs in a protected environment, which is unreachable from the Internet without proper authorization.
It is recommended providing access to VictoriaLogs [data ingestion APIs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
and [querying APIs](https://docs.victoriametrics.com/victorialogs/querying/#http-api) via [vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/)
or similar authorization proxies. See [Security and Load balancing docs](https://docs.victoriametrics.com/victorialogs/security-and-lb/) for details.

It is recommended protecting internal HTTP endpoints from unauthorized access:

- [`/internal/log_new_streams`](https://docs.victoriametrics.com/victorialogs/#logging-new-streams) - via `-logNewStreamsAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/force_flush`](https://docs.victoriametrics.com/victorialogs/#forced-flush) - via `-forceFlushAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/force_merge`](https://docs.victoriametrics.com/victorialogs/#forced-merge) - via `-forceMergeAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).
- [`/internal/partition/*`](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle) - via `-partitionManageAuthKey` [command-line flag](https://docs.victoriametrics.com/victorialogs/#list-of-command-line-flags).

### mTLS

[Enterprise version of VictoriaLogs](https://docs.victoriametrics.com/victoriametrics/enterprise/) supports verification of client TLS certificates
for TCP connections at the address specified via `-httpListenAddr` command-line flag (by default, this is `9428` TCP port).
This is known as [mTLS authentication](https://en.wikipedia.org/wiki/Mutual_authentication#mTLS).

Pass `-mtls` command-line flag to VictoriaLogs in order to enable mTLS authentication for incoming requests.

By default the system-wide [root CA certificates](https://en.wikipedia.org/wiki/Root_certificate) are used for verifying client TLS certificates.
The `-mtlsCAFile` command-line flag can be used for pointing to custom root CA certificates.

[Enterprise version of VictoriaLogs](https://docs.victoriametrics.com/victoriametrics/enterprise/) can be downloaded and evaluated for free
from [the releases page](https://github.com/VictoriaMetrics/VictoriaLogs/releases/latest). See [how to request a free trial license](https://victoriametrics.com/products/enterprise/trial/).

### Automatic issuing of TLS certificates

All the [VictoriaLogs Enterprise](https://docs.victoriametrics.com/victoriametrics/enterprise/) components support automatic issuing of TLS certificates
for public HTTPS server running at `-httpListenAddr` via [Let's Encrypt service](https://letsencrypt.org/).
The following command-line flags must be set in order to enable automatic issuing of TLS certificates:

- `-httpListenAddr` must be set for listening TCP port `443`. For example, `-httpListenAddr=:443`. This port must be accessible by the [Let's Encrypt service](https://letsencrypt.org/).
- `-tls` must be set in order to accept HTTPS requests at `-httpListenAddr`. Note that `-tlcCertFile` and `-tlsKeyFile` aren't needed when automatic TLS certificate issuing is enabled.
- `-tlsAutocertHosts` must be set to comma-separated list of hosts, which can be reached via `-httpListenAddr`. TLS certificates are automatically issued for these hosts.
- `-tlsAutocertEmail` must be set to contact email for the issued TLS certificates.
- `-tlsAutocertCacheDir` may be set to the directory path for persisting the issued TLS certificates between VictoriaMetrics restarts. If this flag isn't set,
  then TLS certificates are re-issued on every restart.

This functionality can be evaluated for free according to [these docs](https://docs.victoriametrics.com/victoriametrics/enterprise/).

See also [security recommendations](https://docs.victoriametrics.com/victorialogs/#security).

## Benchmarks

See the following benchmark results:

- [JSONBench: the comparison of VictoriaLogs with Elasticsearch, MongoDB, DuckDB and PostgreSQL](https://jsonbench.com/#eyJzeXN0ZW0iOnsiQ2xpY2tIb3VzZSAobHo0KSI6ZmFsc2UsIkNsaWNrSG91c2UgKHpzdGQpIjpmYWxzZSwiRHVja0RCIjp0cnVlLCJFbGFzdGljc2VhcmNoIChubyBzb3VyY2UsIGJlc3QgY29tcHJlc3Npb24pIjpmYWxzZSwiRWxhc3RpY3NlYXJjaCAobm8gc291cmNlLCBkZWZhdWx0KSI6ZmFsc2UsIkVsYXN0aWNzZWFyY2ggKGJlc3QgY29tcHJlc3Npb24pIjpmYWxzZSwiRWxhc3RpY3NlYXJjaCAoZGVmYXVsdCkiOnRydWUsIkVsYXN0aWNzZWFyY2giOmZhbHNlLCJNb25nb0RCIChzbmFwcHksIGNvdmVyZWQgaW5kZXgpIjpmYWxzZSwiTW9uZ29EQiAoenN0ZCwgY292ZXJlZCBpbmRleCkiOmZhbHNlLCJNb25nb0RCIChzbmFwcHkpIjpmYWxzZSwiTW9uZ29EQiAoenN0ZCkiOnRydWUsIlBvc3RncmVTUUwgKGx6NCkiOnRydWUsIlBvc3RncmVTUUwgKHBnbHopIjpmYWxzZSwiVmljdG9yaWFMb2dzIjp0cnVlLCJFbGFzdGljc2VhcmNoIChubyBzb3VyY2UsIHpzdGQpIjp0cnVlLCJFbGFzdGljc2VhcmNoIChubyBzb3VyY2UsIGx6NCkiOnRydWUsIkVsYXN0aWNzZWFyY2ggKHpzdGQpIjp0cnVlLCJFbGFzdGljc2VhcmNoIChsejQpIjp0cnVlLCJQb3N0Z3JlU1FMIjp0cnVlfSwic2NhbGUiOjEwMDAwMDAwMDAsIm1ldHJpYyI6ImhvdCIsInF1ZXJpZXMiOlt0cnVlLHRydWUsdHJ1ZSx0cnVlLHRydWVdLCJyZXRhaW5fc3RydWN0dXJlIjp7InllcyI6dHJ1ZSwibm8iOnRydWV9fQ==). The benchmark can be reproduced by running `main.sh` file inside `victorialogs` directory of the [JSONBench repository](https://github.com/ClickHouse/JSONBench).
- [ClickBench: the comparison of VictoriaLogs with Elasticsearch, MongoDB, TimescaleDB, PostgreSQL, MySQL and SQLite](<https://benchmark.clickhouse.com/#system=+ltrc|ehed|noB|yL|gS(|gQ|Lt|m%E2%98%81|%20nu|coog&type=-&machine=-&cluster_size=-&opensource=-&tuned=-&metric=combined&queries=->). The benchmark can be reproduced by running `benchmark.sh` file inside `victorialogs` directory of the [ClickBench repository](https://github.com/ClickHouse/ClickBench/).

Here is a [benchmark suite](https://github.com/VictoriaMetrics/VictoriaLogs/tree/master/deployment/logs-benchmark) for comparing data ingestion performance
and resource usage between VictoriaLogs and Elasticsearch or Loki.

It is recommended [setting up VictoriaLogs](https://docs.victoriametrics.com/victorialogs/quickstart/) in production alongside the existing
log management systems and comparing resource usage + query performance between VictoriaLogs and your system such as Elasticsearch or Grafana Loki.

Please share benchmark results and ideas on how to improve benchmarks / VictoriaLogs
via [VictoriaMetrics community channels](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#community-and-contributions).

## Profiling

VictoriaLogs provides handlers for collecting the following [Go profiles](https://blog.golang.org/profiling-go-programs):

- Memory profile. It can be collected with the following command (replace `0.0.0.0` with hostname if needed):

```sh
curl http://0.0.0.0:9428/debug/pprof/heap > mem.pprof
```

- CPU profile. It can be collected with the following command (replace `0.0.0.0` with hostname if needed):

```sh
curl http://0.0.0.0:9428/debug/pprof/profile > cpu.pprof
```

The command for collecting CPU profile waits for 30 seconds before returning.

The collected profiles may be analyzed with [go tool pprof](https://github.com/google/pprof).
It is safe sharing the collected profiles from security point of view, since they do not contain sensitive information.

## Environment variables

All VictoriaLogs components support configuring command-line flags via environment variables.
You can define flags using environment variables, and you can also
reference environment variables as values, allowing you to reuse or dynamically inject configuration values at application startup.
See [these docs](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#environment-variables) for details.

## Server-side timezone

VictoriaLogs uses server-side timezone in the following cases:

- If the [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field) in the [ingested logs](https://docs.victoriametrics.com/victorialogs/data-ingestion/)
  doesn't contain explicit timezone information.
- If the [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter), [`day_range` filter](https://docs.victoriametrics.com/victorialogs/logsql/#day-range-filter)
  or [`week_range` filter](https://docs.victoriametrics.com/victorialogs/logsql/#week-range-filter) do not contain explicit timezone information.

VictoriaLogs obtains the local timezone from the `TZ` environment variable. It expects valid [IANA Time Zone identifiers](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)
in the `TZ` environment variable. Set `TZ` environment variable to an empty string - `TZ=""` - for using UTC.

## List of command-line flags

Pass `-help` to VictoriaLogs in order to see the list of supported command-line flags with their description:

### Common flags
These flags are available in both VictoriaLogs OSS and VictoriaLogs Enterprise.
{{% content "victoria_logs_common_flags.md" %}}

### Enterprise flags
These flags are available only in [VictoriaLogs enterprise](https://docs.victoriametrics.com/victoriametrics/enterprise/).
{{% content "victoria_logs_enterprise_flags.md" %}}

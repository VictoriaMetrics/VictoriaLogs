---
weight: 21
title: Metrics of vlagent
menu:
  docs:
    parent: victorialogs
    identifier: vlagent-metrics
    weight: 21
    title: Metrics of vlagent
tags:
  - logs
  - metrics
  - monitoring
  - vlagent
aliases:
- /victorialogs/vlagent-metrics.html
- /victorialogs/vlagent-metrics/
---

This document provides a comprehensive reference for all metrics exposed by vlagent at the `http://localhost:9429/metrics` endpoint.
These metrics follow the Prometheus exposition format and can be used for monitoring, alerting, and performance analysis of log collection and remote write operations.

## Remote Write Metrics

### vlagent_remotewrite_block_size_bytes
**Type:** Histogram
**Description:** Compressed size of data blocks after zstd compression before transmission to remote storage. Blocks flushed from memory buffers when reaching `-remoteWrite.maxBlockSize` or during periodic flushes every `-remoteWrite.flushInterval`.

### vlagent_remotewrite_block_size_rows
**Type:** Histogram
**Description:** Number of log entries batched together in each block before compression and transmission. Pending logs flushed to remote storage, shows batching efficiency and optimal buffer sizes.

### vlagent_remotewrite_duration_seconds
**Type:** Histogram
**Labels:**
- `url`: remote storage URL
**Description:** Time spent on individual HTTP requests to remote storage endpoints. Measured from request start to response completion, including network latency and remote processing time.

### vlagent_remotewrite_bytes_sent_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Compressed bytes successfully transmitted to remote storage after receiving 2xx HTTP responses. Only counts confirmed successful deliveries, making it reliable for monitoring actual data throughput.

### vlagent_remotewrite_blocks_sent_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Number of compressed data blocks successfully delivered to remote storage. Only counts successful deliveries after receiving 2xx HTTP responses, shows accurate count of transmission batches.

### vlagent_remotewrite_requests_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
- `status_code`: HTTP status code
**Description:** HTTP requests to remote storage endpoints grouped by response status. Successful requests use `status_code="2XX"`, while failed requests show specific status codes like `400`, `500`, etc.

### vlagent_remotewrite_errors_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Failed HTTP requests to remote storage due to network errors, timeouts, or connection failures. Counted before retry attempts, detects connectivity issues that trigger the retry mechanism.

### vlagent_remotewrite_pending_data_bytes
**Type:** Gauge
**Labels:**
- `path`: file path
- `url`: remote storage URL
**Description:** Total bytes in persistent queue awaiting transmission to remote storage. Includes both in-memory buffers and disk-based queue files. Growing values suggest ingestion rate exceeds transmission capacity or remote storage issues.

### vlagent_remotewrite_pending_inmemory_blocks
**Type:** Gauge
**Labels:**
- `path`: file path
- `url`: remote storage URL
**Description:** Compressed data blocks held in memory before writing to persistent queue files. Higher values consume more memory but provide faster access than disk-based queue. Controlled by memory limits and queue configuration.

### vlagent_remotewrite_queue_blocked
**Type:** Gauge
**Labels:**
- `path`: file path
- `url`: remote storage URL
**Description:** Queue write status where 1 means blocked and 0 means accepting data. Becomes 1 when persistent queue reaches `-remoteWrite.maxDiskUsagePerURL` limit, causing new data to be dropped to prevent disk exhaustion.

### vlagent_remotewrite_rate_limit_reached_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Times transmission was throttled by `-remoteWrite.rateLimit` setting. Data blocks waiting for rate limiter before transmission, shows configured bandwidth limits actively controlling send rates.

### vlagent_remotewrite_rate_limit
**Type:** Gauge
**Labels:**
- `url`: remote storage URL
**Description:** Configured transmission rate limit from `-remoteWrite.rateLimit` setting in bytes per second. Shows 0 when rate limiting is disabled, otherwise displays the maximum allowed transmission speed.

### vlagent_remotewrite_packets_dropped_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Data blocks permanently discarded due to unrecoverable errors. Remote storage returning 400 or 404 status codes, shows client errors or invalid data that cannot be retried.

### vlagent_remotewrite_retries_count_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Retry attempts for failed HTTP requests to remote storage. Each retry cycle triggered by network errors or server failures, with exponential backoff delays controlled by `-remoteWrite.retryMinInterval` and `-remoteWrite.retryMaxTime`.

### vlagent_remotewrite_send_duration_seconds_total
**Type:** Counter
**Labels:**
- `url`: remote storage URL
**Description:** Cumulative time spent in HTTP request processing to remote storage. Includes all request attempts, retries, and network time, shows total transmission overhead for capacity planning and bottleneck analysis.

### vlagent_remotewrite_queues
**Type:** Gauge
**Labels:**
- `url`: remote storage URL
**Description:** Number of parallel transmission workers configured via `-remoteWrite.queues` flag. Higher values provide more concurrent transmission capacity but consume additional memory and connection resources.

## Grafana Dashboards

VictoriaLogs provides official Grafana dashboards that utilize these metrics:
- [vlagent dashboard](https://github.com/VictoriaMetrics/VictoriaLogs/blob/main/dashboards/vlagent.json)

---
weight: 10
title: Metrics Reference
menu:
  docs:
    parent: victorialogs
    identifier: vl-metrics
    weight: 10
    title: Metrics Reference
tags:
  - logs
  - metrics
  - monitoring
aliases:
- /victorialogs/metrics.html
- /victorialogs/metrics/
---

This document provides a comprehensive reference for all metrics exposed by VictoriaLogs at the `/metrics` endpoint. These metrics follow the Prometheus exposition format and can be used for monitoring, alerting, and performance analysis.

## Table of Contents

- [HTTP Request Metrics](#http-request-metrics)
- [Data Ingestion Metrics](#data-ingestion-metrics)
- [Storage System Metrics](#storage-system-metrics)
- [Merge Operation Metrics](#merge-operation-metrics)
- [Query Performance Metrics](#query-performance-metrics)
- [Concurrency and Resource Metrics](#concurrency-and-resource-metrics)
- [Stream and Index Metrics](#stream-and-index-metrics)
- [System Resource Metrics](#system-resource-metrics)
- [Cluster Remote Operation Metrics](#cluster-remote-operation-metrics)
- [Specialized Metrics](#specialized-metrics)
- [Error and Network Metrics](#error-and-network-metrics)

## HTTP Request Metrics

### vl_http_requests_total
**Type:** Counter  
**Labels:** 
- `path`: `/select/logsql/query`, `/insert/jsonline`, `/insert/loki/api/v1/push`, etc.
- `format`: `json`, `protobuf`  
**Description:** HTTP requests arriving at VictoriaLogs endpoints. Counts all requests immediately when received, before any validation or authentication happens.

### vl_http_errors_total
**Type:** Counter  
**Labels:** 
- `path`: endpoint path
- `reason`: `wrong_basic_auth`, `wrong_auth_key`, `unsupported`  
**Description:** Failed HTTP requests due to client errors or data problems. Counts authentication failures, unsupported content types, parameter errors, and cases where request data cannot be parsed (like malformed JSON or invalid log formats).

### vl_http_request_duration_seconds
**Type:** Summary  
**Labels:** 
- `path`: endpoint path  
**Description:** Complete time spent processing each HTTP request from start to finish. Includes all processing steps: parsing request data, validating parameters, storing logs, and sending responses. Captured when requests complete successfully.

### vl_http_request_errors_total
**Type:** Counter  
**Labels:** 
- `path`: endpoint path  
**Description:** Failed request processing in internal cluster endpoints (`/internal/select/*`). Currently only tracks errors for cluster communication endpoints, not public API endpoints like `/select/logsql/query` or `/insert/jsonline`.

## Data Ingestion Metrics

### vl_rows_ingested_total
**Type:** Counter  
**Labels:** 
- `type`: `jsonline`, `loki`, `elasticsearch`, `datadog`, `opentelemetry`, `journald`, `syslog`  
**Description:** Log entries successfully parsed and added to the processing pipeline. Counts all entries that pass initial validation, including debug entries that are processed but not stored when `debug=1` is used.

### vl_bytes_ingested_total
**Type:** Counter  
**Labels:** 
- `type`: ingestion protocol  
**Description:** Estimated JSON size of ingested log entry fields. Calculated using field name lengths and values to provide consistent volume measurement across different input formats like JSON, Loki, or syslog.

### vl_rows_dropped_total
**Type:** Counter  
**Labels:** 
- `reason`: `debug`, `too_many_fields`, `too_big_timestamp`, `too_small_timestamp`  
**Description:** Log entries rejected for specific reasons. `debug` counts entries processed with `debug=1` (parsed but not stored). `too_many_fields` counts entries exceeding `-insert.maxFieldsPerLine`. `too_small_timestamp` counts entries older than `-retentionPeriod`. `too_big_timestamp` counts entries newer than `-futureRetention`.

### vl_insert_flush_duration_seconds
**Type:** Summary  
**Labels:** 
- `type`: ingestion protocol  
**Description:** Time taken to flush accumulated logs from memory buffers to storage. Triggered when buffers fill up or during periodic flushes (every ~1 second with jitter). High values suggest storage bottlenecks or slow disk performance.

### vl_too_long_lines_skipped_total
**Type:** Counter  
**Description:** Log lines exceeding `-insert.maxLineSizeBytes` (default 256KB) during parsing. Lines are skipped to prevent memory exhaustion. Indicates malformed data, overly verbose logs, or need to increase the size limit.

## Storage System Metrics

### vl_data_size_bytes
**Type:** Gauge  
**Labels:** 
- `type`: `storage`, `indexdb`  
**Description:** Current disk space used by log data and indexes. `storage` shows compressed log data across all storage tiers (inmemory + small + big parts). `indexdb` shows space used by search indexes for stream fields and log filtering.

### vl_compressed_data_size_bytes
**Type:** Gauge  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`  
**Description:** Actual compressed size of log data in each storage tier. `inmemory` holds recently ingested data in RAM. `small` contains small parts on disk (typically from recent flushes). `big` contains merged large parts on disk for long-term storage.

### vl_uncompressed_data_size_bytes
**Type:** Gauge  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`  
**Description:** Original uncompressed size of log data in each storage tier before compression. Compare with compressed size to calculate compression ratios and understand storage efficiency across different tiers.

### vl_storage_rows
**Type:** Gauge  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`  
**Description:** Number of log entries in each storage tier. Shows data distribution: `inmemory` has fresh logs not yet written to disk, `small` has recently flushed data, `big` has data from completed merge operations.

### vl_storage_parts
**Type:** Gauge  
**Labels:** 
- `type`: storage tier  
**Description:** Number of storage parts (data files) in each tier. More parts mean fragmentation; fewer parts suggest successful merging. High part counts may slow queries and trigger background merge operations.

### vl_storage_blocks
**Type:** Gauge  
**Labels:** 
- `type`: storage tier  
**Description:** Number of data blocks within storage parts. Blocks are the smallest units of compressed data storage. Higher block counts within parts mean more granular data organization.

### vl_pending_rows
**Type:** Gauge  
**Labels:** 
- `type`: `storage`, `indexdb`  
**Description:** Log entries waiting in memory buffers before being written to disk. `storage` counts log data awaiting flush. `indexdb` counts index entries awaiting flush. High values suggest ingestion rate exceeds storage write speed.

### vl_partitions
**Type:** Gauge  
**Description:** Number of daily partitions currently active in storage. Each partition typically represents one day of log data. Count decreases when old partitions are deleted due to retention policies.

## Merge Operation Metrics

### vl_merge_duration_seconds
**Type:** Summary  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`, `indexdb/inmemory`, `indexdb/file`  
**Description:** Time spent merging storage parts to optimize storage layout and query performance. Background mergers automatically trigger when parts accumulate in each tier. `inmemory` tracks flushing buffered logs to disk, `small` tracks merging recently written files, `big` tracks merging large files for long-term storage.

### vl_merge_bytes
**Type:** Summary  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`, `indexdb/inmemory`, `indexdb/file`  
**Description:** Compressed size of merged storage parts. Resulting part size after merging multiple smaller parts when merge operations complete. Merge efficiency and storage consolidation patterns across different storage tiers.

### vl_active_merges
**Type:** Gauge  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`, `indexdb/inmemory`, `indexdb/file`  
**Description:** Number of merge operations currently running for each storage tier. Increases when background mergers detect parts that need consolidation, decreases when merges complete. High values suggest heavy write load or slow disk performance requiring optimization.

### vl_merges_total
**Type:** Counter  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`, `indexdb/inmemory`, `indexdb/file`  
**Description:** Total completed merge operations since startup. Background mergers consolidate parts to reduce fragmentation and improve query performance. Higher rates suggest active data ingestion and normal storage optimization.

### vl_rows_merged_total
**Type:** Counter  
**Labels:** 
- `type`: `storage/inmemory`, `storage/small`, `storage/big`, `indexdb/inmemory`, `indexdb/file`  
**Description:** Total log entries and index items processed during merge operations. Shows the volume of data being reorganized to maintain optimal storage structure from all input parts when merges complete.

### vl_active_force_merges
**Type:** Counter  
**Description:** Currently active forced merge operations initiated via `/internal/force_merge` API calls. Manual merges that bypass normal merge scheduling and can impact system performance during execution.

## Query Performance Metrics

### vl_storage_per_query_total_read_bytes
**Type:** Histogram  
**Description:** Total disk bytes read during query execution, calculated as the sum of all other per-query read metrics. This represents the total I/O performed when queries complete. Shows query efficiency and helps identify expensive queries that read large amounts of data from storage.

### vl_storage_per_query_values_read_bytes
**Type:** Histogram  
**Description:** Bytes read for actual log field content during queries. The query engine reads compressed log values from disk to match search filters. Higher values suggest queries are accessing many log entries or fields with large content.

### vl_storage_per_query_timestamps_read_bytes
**Type:** Histogram  
**Description:** Bytes read for `_time` field data during time-based filtering. Queries read timestamp blocks from disk to apply time range filters. This affects time-based query performance and time range selection optimization.

### vl_storage_per_query_bloom_filters_read_bytes
**Type:** Histogram  
**Description:** Bytes read for bloom filter data used to skip irrelevant blocks. Queries load bloom filters to quickly eliminate blocks that don't contain search terms. Lower values relative to other metrics suggest effective filtering.

### vl_storage_per_query_block_headers_read_bytes
**Type:** Histogram  
**Description:** Bytes read for block metadata containing compression and structure information. Queries read index blocks to understand data layout before accessing actual content. This shows metadata overhead for query processing.

### vl_storage_per_query_columns_headers_read_bytes
**Type:** Histogram  
**Description:** Bytes read for field name information within each data block. Queries read column headers to identify which fields are present in blocks. Higher values suggest queries are accessing many different log fields.

### vl_storage_per_query_columns_header_indexes_read_bytes
**Type:** Histogram  
**Description:** Bytes read for indexes that locate field information within column headers. Queries read column header index data to efficiently find specific field locations. This shows overhead of field-based query operations.

### vl_storage_per_query_processed_blocks
**Type:** Histogram  
**Description:** Number of data blocks processed during query execution. This counts each block that passes initial filtering and requires detailed processing. High values suggest queries are scanning many blocks and may need better time ranges or filters.

## Concurrency and Resource Metrics

### vl_concurrent_select_limit_reached_total
**Type:** Counter  
**Description:** Query requests hitting the concurrency limit and waiting for available execution slots. Indicates the system has reached `-search.maxConcurrentRequests` capacity and new queries are queued until running queries complete.

### vl_concurrent_select_limit_timeout_total
**Type:** Counter  
**Description:** Queries dropped after waiting longer than `-search.maxQueueDuration` for execution slots. Indicates severe sustained overload where the query queue exceeds acceptable wait times.

### vl_concurrent_select_capacity
**Type:** Gauge  
**Description:** Configured maximum concurrent query limit from `-search.maxConcurrentRequests` flag. Total number of queries that can execute simultaneously before new queries must wait in queue.

### vl_concurrent_select_current
**Type:** Gauge  
**Description:** Current number of queries actively executing. Real-time query processing load when the system approaches the `-search.maxConcurrentRequests` capacity limit.

### vl_insert_processors_count
**Type:** Gauge  
**Description:** Number of active processors currently handling data ingestion from different sources. Current ingestion pipeline utilization as streams start and finish processing.

## Stream and Index Metrics

### vl_streams_created_total
**Type:** Counter  
**Description:** New unique combinations of stream fields first encountered during log ingestion. Only counts streams not previously seen since startup, shows growth in stream cardinality and high-cardinality detection.

### vl_indexdb_rows
**Type:** Gauge  
**Description:** Total index entries stored for stream field lookups and filtering. Includes both in-memory and file-based index entries that enable fast stream discovery and log field searches. Growth shows more indexed content.

### vl_indexdb_parts
**Type:** Gauge  
**Description:** Number of index storage parts containing stream and field metadata. Combines in-memory and file-based parts that store indexing information. Higher counts may suggest fragmentation requiring background merge operations.

### vl_indexdb_blocks
**Type:** Gauge  
**Description:** Total index blocks storing compressed index data. Smallest units of index storage across in-memory and file-based components. Index storage organization and efficiency.

## System Resource Metrics

### vl_free_disk_space_bytes
**Type:** Gauge  
**Labels:** 
- `path`: storage directory  
**Description:** Available disk space remaining at the storage location. Disk space exhaustion triggers read-only mode or causes ingestion failures. Monitor against `-storage.minFreeDiskSpaceBytes` threshold.

### vl_total_disk_space_bytes
**Type:** Gauge  
**Labels:** 
- `path`: storage directory  
**Description:** Total filesystem capacity at the storage location. Used with free space metrics to calculate disk utilization percentages and plan capacity expansion before hitting retention limits.

### vl_max_disk_space_usage_bytes
**Type:** Gauge  
**Labels:** 
- `path`: storage directory  
**Description:** Current retention limit for automatic partition cleanup. Shows either `-retention.maxDiskSpaceUsageBytes` value or calculated percentage of total disk space from `-retention.maxDiskUsagePercent`. When exceeded, oldest partitions are automatically deleted.

### vl_storage_is_read_only
**Type:** Gauge  
**Labels:** 
- `path`: storage directory  
**Description:** Storage write protection status where 1 means read-only mode and 0 means normal operation. Automatically set to 1 when free disk space falls below `-storage.minFreeDiskSpaceBytes` to prevent disk exhaustion.

## Cluster Remote Operation Metrics

### vl_insert_remote_send_errors_total
**Type:** Counter  
**Labels:** 
- `addr`: storage node address  
**Description:** Failed data ingestion attempts to remote storage nodes. Network errors, authentication failures, non-2xx HTTP responses, or when nodes are temporarily disabled after errors. Indicates cluster connectivity or remote node issues.

### vl_insert_remote_is_reachable
**Type:** Gauge  
**Labels:** 
- `addr`: storage node address  
**Description:** Remote storage node availability status where 1 means reachable and 0 means unreachable. Becomes 0 when send errors occur and temporarily disabled for 10 seconds, returns to 1 when successful requests resume. Cluster health monitoring.

### vl_select_remote_send_errors_total
**Type:** Counter  
**Labels:** 
- `addr`: storage node address  
**Description:** Failed query forwarding attempts to remote storage nodes. These are query execution failures due to network issues, timeouts, or remote node problems. Does not include cancelled queries, only actual communication failures.

### vl_insert_active_streams
**Type:** Gauge  
**Description:** Unique log streams held in memory for cluster load balancing. Accumulates all stream combinations seen since vlinsert startup and never decreases. Higher values consume more memory and show stream diversity requiring cluster distribution tracking.

## Specialized Metrics

### vl_live_tailing_requests
**Type:** Counter  
**Description:** Client connections to `/select/logsql/tail` endpoint for real-time log streaming. Each request establishes a persistent connection that bypasses normal query concurrency limits and timeouts.

## Error and Network Metrics

### vl_errors_total
**Type:** Counter  
**Labels:** 
- `type`: `syslog`  
**Description:** Syslog parsing errors encountered during log line processing. Individual syslog messages that fail to parse due to malformed timestamps, invalid priorities, or other RFC3164/RFC5424 format violations. Syslog data quality monitoring.

### vl_udp_requests_total
**Type:** Counter  
**Labels:** 
- `type`: `syslog`  
**Description:** UDP packets received at syslog endpoints configured via `-syslog.listenAddr.udp`. Total network traffic volume to syslog UDP listeners regardless of content validity.

### vl_udp_errors_total
**Type:** Counter  
**Labels:** 
- `type`: `syslog`  
**Description:** UDP network errors at syslog endpoints including temporary network failures, connection resets, and socket read failures. Excludes parsing errors which are tracked separately. UDP network connectivity issues.

## Grafana Dashboards

VictoriaLogs provides official Grafana dashboards that utilize these metrics:
- [Single-node dashboard](https://github.com/VictoriaMetrics/VictoriaLogs/blob/main/dashboards/victorialogs.json)
- [Cluster dashboard](https://github.com/VictoriaMetrics/VictoriaLogs/blob/main/dashboards/victorialogs-cluster.json)

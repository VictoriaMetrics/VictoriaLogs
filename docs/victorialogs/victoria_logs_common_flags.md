---
build:
  list: never
  publishResources: false
  render: never
sitemap:
  disable: true
---
<!-- The file has to be manually updated during feature work in PR, make docs-update-flags command could be used peridically to ensure the flags in sync. -->
```shellhelp

victoria-logs is a log management and analytics service.

See the docs at https://docs.victoriametrics.com/victorialogs/

  -blockcache.missesBeforeCaching int
     The number of cache misses before putting the block into cache. Higher values may reduce indexdb/dataBlocks cache size at the cost of higher CPU and disk read usage (default 2)
  -datadog.ignoreFields array
     Comma-separated list of fields to ignore for logs ingested via DataDog protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/datadog-agent/#dropping-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -datadog.maxRequestSize size
     The maximum size in bytes of a single DataDog request
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 67108864)
  -datadog.streamFields array
     Comma-separated list of fields to use as log stream fields for logs ingested via DataDog protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/datadog-agent/#stream-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -defaultMsgValue string
     Default value for _msg field if the ingested log entry doesn't contain it; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field (default "missing _msg field; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field")
  -defaultParallelReaders int
     Default number of parallel data readers to use for executing every query; higher number of readers may help increasing query performance on high-latency storage such as NFS or S3 at the cost of higher RAM usage; see https://docs.victoriametrics.com/victorialogs/logsql/#parallel_readers-query-option (default 2x CPU cores)
  -delete.enable
     Whether to enable /delete/* HTTP endpoints; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs
  -elasticsearch.version string
     Elasticsearch version to report to client (default "8.9.0")
  -enableTCP6
     Whether to enable IPv6 for listening and dialing. By default, only IPv4 TCP and UDP are used
  -envflag.enable
     Whether to enable reading flags from environment variables in addition to the command line. Command line flag values have priority over values from environment vars. Flags are read only from the command line if this flag isn't set. See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#environment-variables for more details
  -envflag.prefix string
     Prefix for environment variables if -envflag.enable is set
  -filestream.disableFadvise
     Whether to disable fadvise() syscall when reading large data files. The fadvise() syscall prevents from eviction of recently accessed data from OS page cache during background merges and backups. In some rare cases it is better to disable the syscall if it uses too much CPU
  -flagsAuthKey value
     Auth key for /flags endpoint. It must be passed via authKey query arg. It overrides -httpAuth.*
     Flag value can be read from the given file when using -flagsAuthKey=file:///abs/path/to/file or -flagsAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -flagsAuthKey=http://host/path or -flagsAuthKey=https://host/path
  -forceFlushAuthKey value
     authKey, which must be passed in query string to /internal/force_flush . It overrides -httpAuth.* . See https://docs.victoriametrics.com/victorialogs/#forced-flush
     Flag value can be read from the given file when using -forceFlushAuthKey=file:///abs/path/to/file or -forceFlushAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -forceFlushAuthKey=http://host/path or -forceFlushAuthKey=https://host/path
  -forceMergeAuthKey value
     authKey, which must be passed in query string to /internal/force_merge . It overrides -httpAuth.* . See https://docs.victoriametrics.com/victorialogs/#forced-merge
     Flag value can be read from the given file when using -forceMergeAuthKey=file:///abs/path/to/file or -forceMergeAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -forceMergeAuthKey=http://host/path or -forceMergeAuthKey=https://host/path
  -fs.disableMmap
     Whether to use pread() instead of mmap() for reading data files. By default, mmap() is used for 64-bit arches and pread() is used for 32-bit arches, since they cannot read data files bigger than 2^32 bytes in memory. mmap() is usually faster for reading small data chunks than pread()
  -fs.maxConcurrency int
     The maximum number of concurrent goroutines to work with files; smaller values may help reducing Go scheduling latency on systems with small number of CPU cores; higher values may help reducing data ingestion latency on systems with high-latency storage such as NFS or Ceph (default 16x CPU cores, default capped at 256)
  -futureRetention value
     Log entries with timestamps bigger than now+futureRetention are rejected during data ingestion; see https://docs.victoriametrics.com/victorialogs/#retention
     The following optional suffixes are supported: s (second), h (hour), d (day), w (week), M (month), y (year). If suffix isn't set, then the duration is counted in months (default 2d)
  -http.connTimeout duration
     Incoming connections to -httpListenAddr are closed after the configured timeout. This may help evenly spreading load among a cluster of services behind TCP-level load balancer. Zero value disables closing of incoming connections (default 2m0s)
  -http.disableCORS
     Disable CORS for all origins (*)
  -http.disableKeepAlive
     Whether to disable HTTP keep-alive for incoming connections at -httpListenAddr
  -http.disableResponseCompression
     Disable compression of HTTP responses to save CPU resources. By default, compression is enabled to save network bandwidth
  -http.header.csp string
     Value for 'Content-Security-Policy' header, recommended: "default-src 'self'"
  -http.header.frameOptions string
     Value for 'X-Frame-Options' header
  -http.header.hsts string
     Value for 'Strict-Transport-Security' header, recommended: 'max-age=31536000; includeSubDomains'
  -http.idleConnTimeout duration
     Timeout for incoming idle http connections (default 1m0s)
  -http.maxGracefulShutdownDuration duration
     The maximum duration for a graceful shutdown of the HTTP server. A highly loaded server may require increased value for a graceful shutdown (default 7s)
  -http.pathPrefix string
     An optional prefix to add to all the paths handled by http server. For example, if '-http.pathPrefix=/foo/bar' is set, then all the http requests will be handled on '/foo/bar/*' paths. This may be useful for proxied requests. See https://www.robustperception.io/using-external-urls-and-proxies-with-prometheus
  -http.shutdownDelay duration
     Optional delay before http server shutdown. During this delay, the server returns non-OK responses from /health page, so load balancers can route new requests to other servers
  -httpAuth.password value
     Password for HTTP server's Basic Auth. The authentication is disabled if -httpAuth.username is empty
     Flag value can be read from the given file when using -httpAuth.password=file:///abs/path/to/file or -httpAuth.password=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -httpAuth.password=http://host/path or -httpAuth.password=https://host/path
  -httpAuth.username string
     Username for HTTP server's Basic Auth. The authentication is disabled if empty. See also -httpAuth.password
  -httpListenAddr array
     TCP address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -httpListenAddr.useProxyProtocol array
     Whether to use proxy protocol for connections accepted at the given -httpListenAddr . See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -inmemoryDataFlushInterval duration
     The interval for guaranteed saving of in-memory data to disk. The saved data survives unclean shutdowns such as OOM crash, hardware reset, SIGKILL, etc. Bigger intervals may help increase the lifetime of flash storage with limited write cycles (e.g. Raspberry PI). Smaller intervals increase disk IO load. Minimum supported value is 1s (default 5s)
  -insert.concurrency int
     The average number of concurrent data ingestion requests, which can be sent to every -storageNode (default 2)
  -insert.disable
     Whether to disable /insert/* HTTP endpoints
  -insert.disableCompression
     Whether to disable compression when sending the ingested data to -storageNode nodes. Disabled compression reduces CPU usage at the cost of higher network usage
  -insert.maxFieldsPerLine int
     The maximum number of log fields per line, which can be read by /insert/* handlers; see https://docs.victoriametrics.com/victorialogs/faq/#how-many-fields-a-single-log-entry-may-contain (default 1000)
  -insert.maxLineSizeBytes size
     The maximum size of a single line that can be read by /insert/* handlers. Regardless of this flag, entries above the 2 MB limit are ignored, see https://docs.victoriametrics.com/victorialogs/faq/#what-length-a-log-record-is-expected-to-have
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 262144)
  -insert.maxQueueDuration duration
     The maximum duration to wait in the queue when -maxConcurrentInserts concurrent insert requests are executed (default 1m0s)
  -internStringCacheExpireDuration duration
     The expiry duration for caches for interned strings. See https://en.wikipedia.org/wiki/String_interning . See also -internStringMaxLen and -internStringDisableCache (default 6m0s)
  -internStringDisableCache
     Whether to disable caches for interned strings. This may reduce memory usage at the cost of higher CPU usage. See https://en.wikipedia.org/wiki/String_interning . See also -internStringCacheExpireDuration and -internStringMaxLen
  -internStringMaxLen int
     The maximum length for strings to intern. A lower limit may save memory at the cost of higher CPU usage. See https://en.wikipedia.org/wiki/String_interning . See also -internStringDisableCache and -internStringCacheExpireDuration (default 500)
  -internaldelete.enable
     Whether to enable /internal/delete/* HTTP endpoints, which are used by vlselect for deleting logs via delete API at vlstorage nodes; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs
  -internalinsert.disable
     Whether to disable /internal/insert HTTP endpoint. See https://docs.victoriametrics.com/victorialogs/cluster/#security
  -internalinsert.maxRequestSize size
     The maximum size in bytes of a single request, which can be accepted at /internal/insert HTTP endpoint
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 67108864)
  -internalselect.disable
     Whether to disable /internal/select/* HTTP endpoints
  -internalselect.maxConcurrentRequests int
     The limit on the number of concurrent requests to /internal/select/* endpoints; other requests are put into the wait queue; see https://docs.victoriametrics.com/victorialogs/cluster/ (default 100)
  -journald.ignoreFields array
     Comma-separated list of fields to ignore for logs ingested over journald protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#dropping-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -journald.includeEntryMetadata
     Include Journald fields with double underscore prefixes
  -journald.streamFields array
     Comma-separated list of fields to use as log stream fields for logs ingested over journald protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#stream-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -journald.tenantID string
     TenantID for logs ingested via the Journald endpoint. See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#multitenancy (default "0:0")
  -journald.timeField string
     Field to use as a log timestamp for logs ingested via journald protocol. See https://docs.victoriametrics.com/victorialogs/data-ingestion/journald/#time-field (default "__REALTIME_TIMESTAMP")
  -logIngestedRows
     Whether to log all the ingested log entries; this can be useful for debugging of data ingestion; see https://docs.victoriametrics.com/victorialogs/data-ingestion/ ; see also -logNewStreams
  -logNewStreams
     Whether to log creation of new streams; this can be useful for debugging of high cardinality issues with log streams; see https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields ; see also -logIngestedRows
  -logNewStreamsAuthKey value
     authKey, which must be passed in query string to /internal/log_new_streams . It overrides -httpAuth.* . See https://docs.victoriametrics.com/victorialogs/#logging-new-streams
     Flag value can be read from the given file when using -logNewStreamsAuthKey=file:///abs/path/to/file or -logNewStreamsAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -logNewStreamsAuthKey=http://host/path or -logNewStreamsAuthKey=https://host/path
  -loggerDisableTimestamps
     Whether to disable writing timestamps in logs
  -loggerErrorsPerSecondLimit int
     Per-second limit on the number of ERROR messages. If more than the given number of errors are emitted per second, the remaining errors are suppressed. Zero values disable the rate limit
  -loggerFormat string
     Format for logs. Possible values: default, json (default "default")
  -loggerJSONFields string
     Allows renaming fields in JSON formatted logs. Example: "ts:timestamp,msg:message" renames "ts" to "timestamp" and "msg" to "message". Supported fields: ts, level, caller, msg
  -loggerLevel string
     Minimum level of errors to log. Possible values: INFO, WARN, ERROR, FATAL, PANIC (default "INFO")
  -loggerMaxArgLen int
     The maximum length of a single logged argument. Longer arguments are replaced with 'arg_start..arg_end', where 'arg_start' and 'arg_end' is prefix and suffix of the arg with the length not exceeding -loggerMaxArgLen / 2 (default 5000)
  -loggerOutput string
     Output for the logs. Supported values: stderr, stdout (default "stderr")
  -loggerTimezone string
     Timezone to use for timestamps in logs. Timezone must be a valid IANA Time Zone. For example: America/New_York, Europe/Berlin, Etc/GMT+3 or Local (default "UTC")
  -loggerWarnsPerSecondLimit int
     Per-second limit on the number of WARN messages. If more than the given number of warns are emitted per second, then the remaining warns are suppressed. Zero values disable the rate limit
  -loki.disableMessageParsing
     Whether to disable automatic parsing of JSON-encoded log fields inside Loki log message into distinct log fields
  -loki.maxRequestSize size
     The maximum size in bytes of a single Loki request
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 67108864)
  -maxBackfillAge value
     Log entries with timestamps older than now-maxBackfillAge are rejected during data ingestion; see https://docs.victoriametrics.com/victorialogs/#backfilling
     The following optional suffixes are supported: s (second), h (hour), d (day), w (week), M (month), y (year). If suffix isn't set, then the duration is counted in months (default 0)
  -maxConcurrentInserts int
     The maximum number of concurrent insert requests. Set higher value when clients send data over slow networks. Default value depends on the number of available CPU cores. It should work fine in most cases since it minimizes resource usage. See also -insert.maxQueueDuration (default 2x CPU cores)
  -memory.allowedBytes size
     Allowed size of system memory VictoriaMetrics caches may occupy. This option overrides -memory.allowedPercent if set to a non-zero value. Too low a value may increase the cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache resulting in higher disk IO usage
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 0)
  -memory.allowedPercent float
     Allowed percent of system memory VictoriaMetrics caches may occupy. See also -memory.allowedBytes. Too low a value may increase cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache which will result in higher disk IO usage (default 60)
  -metrics.exposeMetadata
     Whether to expose TYPE and HELP metadata at the /metrics page, which is exposed at -httpListenAddr . The metadata may be needed when the /metrics page is consumed by systems, which require this information. For example, Managed Prometheus in Google Cloud - https://cloud.google.com/stackdriver/docs/managed-prometheus/troubleshooting#missing-metric-type
  -metricsAuthKey value
     Auth key for /metrics endpoint. It must be passed via authKey query arg. It overrides -httpAuth.*
     Flag value can be read from the given file when using -metricsAuthKey=file:///abs/path/to/file or -metricsAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -metricsAuthKey=http://host/path or -metricsAuthKey=https://host/path
  -nativeinsert.maxRequestSize size
     The maximum size in bytes of a single request, which can be accepted at /insert/native HTTP endpoint
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 67108864)
  -opentelemetry.maxRequestSize size
     The maximum size in bytes of a single OpenTelemetry request
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 67108864)
  -partitionManageAuthKey value
     authKey, which must be passed in query string to /internal/partition/* . It overrides -httpAuth.* . See https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle
     Flag value can be read from the given file when using -partitionManageAuthKey=file:///abs/path/to/file or -partitionManageAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -partitionManageAuthKey=http://host/path or -partitionManageAuthKey=https://host/path
  -pprofAuthKey value
     Auth key for /debug/pprof/* endpoints. It must be passed via authKey query arg. It overrides -httpAuth.*
     Flag value can be read from the given file when using -pprofAuthKey=file:///abs/path/to/file or -pprofAuthKey=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -pprofAuthKey=http://host/path or -pprofAuthKey=https://host/path
  -pushmetrics.disableCompression
     Whether to disable request body compression when pushing metrics to every -pushmetrics.url
  -pushmetrics.extraLabel array
     Optional labels to add to metrics pushed to every -pushmetrics.url . For example, -pushmetrics.extraLabel='instance="foo"' adds instance="foo" label to all the metrics pushed to every -pushmetrics.url
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -pushmetrics.header array
     Optional HTTP request header to send to every -pushmetrics.url . For example, -pushmetrics.header='Authorization: Basic foobar' adds 'Authorization: Basic foobar' header to every request to every -pushmetrics.url
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -pushmetrics.interval duration
     Interval for pushing metrics to every -pushmetrics.url (default 10s)
  -pushmetrics.url array
     Optional URL to push metrics exposed at /metrics page. See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#push-metrics . By default, metrics exposed at /metrics page aren't pushed to any remote storage
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -retention.maxDiskSpaceUsageBytes size
     The maximum disk space usage at -storageDataPath before older per-day partitions are automatically dropped; see https://docs.victoriametrics.com/victorialogs/#retention-by-disk-space-usage ; see also -retentionPeriod
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 0)
  -retention.maxDiskUsagePercent int
     The maximum allowed disk usage percentage (1-100) for the filesystem that contains -storageDataPath before older per-day partitions are automatically dropped; mutually exclusive with -retention.maxDiskSpaceUsageBytes; see https://docs.victoriametrics.com/victorialogs/#retention-by-disk-space-usage-percent
  -retentionPeriod value
     Log entries with timestamps older than now-retentionPeriod are automatically deleted; log entries with timestamps outside the retention are also rejected during data ingestion; the minimum supported retention is 1d (one day); see https://docs.victoriametrics.com/victorialogs/#retention ; see also -retention.maxDiskSpaceUsageBytes and -retention.maxDiskUsagePercent
     The following optional suffixes are supported: s (second), h (hour), d (day), w (week), M (month), y (year). If suffix isn't set, then the duration is counted in months (default 7d)
  -search.allowPartialResponse
     Whether to allow returning partial responses when some of vlstorage nodes from the -storageNode list are unavailable for querying. This flag works only for cluster setup of VictoriaLogs. See https://docs.victoriametrics.com/victorialogs/querying/#partial-responses
  -search.logSlowQueryDuration duration
     Log queries with execution time exceeding this value. Zero disables slow query logging (default 5s)
  -search.maxConcurrentRequests int
     The maximum number of concurrent search requests. It shouldn't be high, since a single request can saturate all the CPU cores, while many concurrently executed requests may require high amounts of memory. See also -search.maxQueueDuration (default vlselect.getDefaultMaxConcurrentRequests())
  -search.maxQueryDuration duration
     The maximum duration for query execution. It can be overridden to a smaller value on a per-query basis via 'timeout' query arg (default 30s)
  -search.maxQueryTimeRange value
     The maximum time range, which can be set in the query sent to querying APIs. Queries with bigger time ranges are rejected. See https://docs.victoriametrics.com/victorialogs/querying/#resource-usage-limits
     The following unit suffixes are required: s (second), m (minute), h (hour), d (day), w (week), y (year). Bare numbers without units are not allowed (except 0) (default 0)
  -search.maxQueueDuration duration
     The maximum time the search request waits for execution when -search.maxConcurrentRequests limit is reached; see also -search.maxQueryDuration (default 10s)
  -secret.flags array
     Comma-separated list of flag names with secret values. Values for these flags are hidden in logs and on /metrics page
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -select.disable
     Whether to disable /select/* HTTP endpoints
  -select.disableCompression
     Whether to disable compression for select query responses received from -storageNode nodes. Disabled compression reduces CPU usage at the cost of higher network usage
  -storage.minFreeDiskSpaceBytes size
     The minimum free disk space at -storageDataPath after which the storage stops accepting new data
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 10000000)
  -storageDataPath string
     Path to directory where to store VictoriaLogs data; see https://docs.victoriametrics.com/victorialogs/#storage (default "victoria-logs-data")
  -storageNode array
     Comma-separated list of TCP addresses for storage nodes to route the ingested logs to and to send select queries to. If the list is empty, then the ingested logs are stored and queried locally from -storageDataPath
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.bearerToken array
     Optional bearer auth token to use for the corresponding -storageNode
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.bearerTokenFile array
     Optional path to bearer token file to use for the corresponding -storageNode. The token is re-read from the file every second
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.password array
     Optional basic auth password to use for the corresponding -storageNode
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.passwordFile array
     Optional path to basic auth password to use for the corresponding -storageNode. The file is re-read every second
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.tls array
     Whether to use TLS (HTTPS) protocol for communicating with the corresponding -storageNode. By default communication is performed via HTTP
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -storageNode.tlsCAFile array
     Optional path to TLS CA file to use for verifying connections to the corresponding -storageNode. By default, system CA is used
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.tlsCertFile array
     Optional path to client-side TLS certificate file to use when connecting to the corresponding -storageNode
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.tlsInsecureSkipVerify array
     Whether to skip tls verification when connecting to the corresponding -storageNode
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -storageNode.tlsKeyFile array
     Optional path to client-side TLS certificate key to use when connecting to the corresponding -storageNode
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.tlsServerName array
     Optional TLS server name to use for connections to the corresponding -storageNode. By default, the server name from -storageNode is used
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.username array
     Optional basic auth username to use for the corresponding -storageNode
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -storageNode.usernameFile array
     Optional path to basic auth username to use for the corresponding -storageNode. The file is re-read every second
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.compressMethod.tcp array
     Compression method for syslog messages received at the corresponding -syslog.listenAddr.tcp. Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.compressMethod.udp array
     Compression method for syslog messages received at the corresponding -syslog.listenAddr.udp. Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.compressMethod.unix array
     Compression method for syslog messages received at the corresponding -syslog.listenAddr.unix. Supported values: none, gzip, deflate. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#compression
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.decolorizeFields.tcp array
     Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.decolorizeFields.udp array
     Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.decolorizeFields.unix array
     Fields to remove ANSI color codes across logs ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#decolorizing-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.extraFields.tcp array
     Fields to add to logs ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.extraFields.udp array
     Fields to add to logs ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.extraFields.unix array
     Fields to add to logs ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#adding-extra-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.ignoreFields.tcp array
     Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.ignoreFields.udp array
     Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.ignoreFields.unix array
     Fields to ignore at logs ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#dropping-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.listenAddr.tcp array
     Comma-separated list of TCP addresses to listen to for Syslog messages. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.listenAddr.udp array
     Comma-separated list of UDP addresses to listen to for Syslog messages. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.listenAddr.unix array
     Comma-separated list of Unix socket filepaths to listen to for Syslog messages. Filepaths may be prepended with 'unixgram:'  for listening for SOCK_DGRAM sockets. By default SOCK_STREAM sockets are used. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.streamFields.tcp array
     Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.streamFields.udp array
     Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.streamFields.unix array
     Fields to use as log stream labels for logs ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#stream-fields
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tenantID.tcp array
     TenantID for logs ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tenantID.udp array
     TenantID for logs ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tenantID.unix array
     TenantID for logs ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#multitenancy
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.timezone string
     Timezone to use when parsing timestamps in RFC3164 syslog messages. Timezone must be a valid IANA Time Zone. For example: America/New_York, Europe/Berlin, Etc/GMT+3 . See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/ (default "Local")
  -syslog.tls array
     Whether to enable TLS for receiving syslog messages at the corresponding -syslog.listenAddr.tcp. The corresponding -syslog.tlsCertFile and -syslog.tlsKeyFile must be set if -syslog.tls is set. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.tlsCertFile array
     Path to file with TLS certificate for the corresponding -syslog.listenAddr.tcp if the corresponding -syslog.tls is set. Prefer ECDSA certs instead of RSA certs as RSA certs are slower. The provided certificate file is automatically re-read every second, so it can be dynamically updated. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tlsCipherSuites array
     Optional list of TLS cipher suites for -syslog.listenAddr.tcp if -syslog.tls is set. See the list of supported cipher suites at https://pkg.go.dev/crypto/tls#pkg-constants . See also https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tlsKeyFile array
     Path to file with TLS key for the corresponding -syslog.listenAddr.tcp if the corresponding -syslog.tls is set. The provided key file is automatically re-read every second, so it can be dynamically updated. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -syslog.tlsMinVersion string
     The minimum TLS version to use for -syslog.listenAddr.tcp if -syslog.tls is set. Supported values: TLS10, TLS11, TLS12, TLS13. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#security (default "TLS13")
  -syslog.useLocalTimestamp.tcp array
     Whether to use local timestamp instead of the original timestamp for the ingested syslog messages at the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.useLocalTimestamp.udp array
     Whether to use local timestamp instead of the original timestamp for the ingested syslog messages at the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.useLocalTimestamp.unix array
     Whether to use local timestamp instead of the original timestamp for the ingested syslog messages at the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#log-timestamps
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.useRemoteIP.tcp array
     Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested via the corresponding -syslog.listenAddr.tcp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.useRemoteIP.udp array
     Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested via the corresponding -syslog.listenAddr.udp. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -syslog.useRemoteIP.unix array
     Whether to add remote ip address as 'remote_ip' log field for syslog messages ingested via the corresponding -syslog.listenAddr.unix. See https://docs.victoriametrics.com/victorialogs/data-ingestion/syslog/#capturing-remote-ip-address
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -tls array
     Whether to enable TLS for incoming HTTP requests at the given -httpListenAddr (aka https). -tlsCertFile and -tlsKeyFile must be set if -tls is set. See also -mtls
     Supports array of values separated by comma or specified via multiple flags.
     Empty values are set to false.
  -tlsCertFile array
     Path to file with TLS certificate for the corresponding -httpListenAddr if -tls is set. Prefer ECDSA certs instead of RSA certs as RSA certs are slower. The provided certificate file is automatically re-read every second, so it can be dynamically updated. See also -tlsAutocertHosts
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -tlsCipherSuites array
     Optional list of TLS cipher suites for incoming requests over HTTPS if -tls is set. See the list of supported cipher suites at https://pkg.go.dev/crypto/tls#pkg-constants
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -tlsKeyFile array
     Path to file with TLS key for the corresponding -httpListenAddr if -tls is set. The provided key file is automatically re-read every second, so it can be dynamically updated. See also -tlsAutocertHosts
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -tlsMinVersion array
     Optional minimum TLS version to use for the corresponding -httpListenAddr if -tls is set. Supported values: TLS10, TLS11, TLS12, TLS13
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -version
     Show VictoriaMetrics version
```

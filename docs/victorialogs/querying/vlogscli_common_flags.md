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

vlogscli is a command-line tool for querying VictoriaLogs.

See the docs at https://docs.victoriametrics.com/victorialogs/querying/vlogscli/

  -accountID int
     Account ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy
  -bearerToken value
     Optional bearer auth token to use for the -datasource.url
     Flag value can be read from the given file when using -bearerToken=file:///abs/path/to/file or -bearerToken=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -bearerToken=http://host/path or -bearerToken=https://host/path
  -blockcache.missesBeforeCaching int
     The number of cache misses before putting the block into cache. Higher values may reduce indexdb/dataBlocks cache size at the cost of higher CPU and disk read usage (default 2)
  -datasource.url string
     URL for querying VictoriaLogs; see https://docs.victoriametrics.com/victorialogs/querying/#querying-logs . See also -tail.url (default "http://localhost:9428/select/logsql/query")
  -enableTCP6
     Whether to enable IPv6 for listening and dialing. By default, only IPv4 TCP and UDP are used
  -envflag.enable
     Whether to enable reading flags from environment variables in addition to the command line. Command line flag values have priority over values from environment vars. Flags are read only from the command line if this flag isn't set. See https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#environment-variables for more details
  -envflag.prefix string
     Prefix for environment variables if -envflag.enable is set
  -filestream.disableFadvise
     Whether to disable fadvise() syscall when reading large data files. The fadvise() syscall prevents from eviction of recently accessed data from OS page cache during background merges and backups. In some rare cases it is better to disable the syscall if it uses too much CPU
  -fs.disableMmap
     Whether to use pread() instead of mmap() for reading data files. By default, mmap() is used for 64-bit arches and pread() is used for 32-bit arches, since they cannot read data files bigger than 2^32 bytes in memory. mmap() is usually faster for reading small data chunks than pread()
  -fs.maxConcurrency int
     The maximum number of concurrent goroutines to work with files; smaller values may help reducing Go scheduling latency on systems with small number of CPU cores; higher values may help reducing data ingestion latency on systems with high-latency storage such as NFS or Ceph (default 16x CPU cores, default capped at 256)
  -header array
     Optional header to pass in request -datasource.url in the form 'HeaderName: value'
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -historyFile string
     Path to file with command history (default "vlogscli-history")
  -internStringCacheExpireDuration duration
     The expiry duration for caches for interned strings. See https://en.wikipedia.org/wiki/String_interning . See also -internStringMaxLen and -internStringDisableCache (default 6m0s)
  -internStringDisableCache
     Whether to disable caches for interned strings. This may reduce memory usage at the cost of higher CPU usage. See https://en.wikipedia.org/wiki/String_interning . See also -internStringCacheExpireDuration and -internStringMaxLen
  -internStringMaxLen int
     The maximum length for strings to intern. A lower limit may save memory at the cost of higher CPU usage. See https://en.wikipedia.org/wiki/String_interning . See also -internStringDisableCache and -internStringCacheExpireDuration (default 500)
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
  -memory.allowedBytes size
     Allowed size of system memory VictoriaMetrics caches may occupy. This option overrides -memory.allowedPercent if set to a non-zero value. Too low a value may increase the cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache resulting in higher disk IO usage
     Supports the following optional suffixes for size values: KB, MB, GB, TB, KiB, MiB, GiB, TiB (default 0)
  -memory.allowedPercent float
     Allowed percent of system memory VictoriaMetrics caches may occupy. See also -memory.allowedBytes. Too low a value may increase cache miss rate usually resulting in higher CPU and disk IO usage. Too high a value may evict too much data from the OS page cache which will result in higher disk IO usage (default 60)
  -password value
     Optional basic auth password to use for the -datasource.url
     Flag value can be read from the given file when using -password=file:///abs/path/to/file or -password=file://./relative/path/to/file.
     Flag value can be read from the given http/https url when using -password=http://host/path or -password=https://host/path
  -projectID int
     Project ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy
  -secret.flags array
     Comma-separated list of flag names with secret values. Values for these flags are hidden in logs and on /metrics page
     Supports an array of values separated by comma or specified via multiple flags.
     Each array item can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
  -tail.url string
     URL for live tailing queries to VictoriaLogs; see https://docs.victoriametrics.com/victorialogs/querying/#live-tailing .The url is automatically detected from -datasource.url by replacing /query with /tail at the end if -tail.url is empty
  -tlsCAFile string
     Optional path to TLS CA file to use for verifying connections to the -datasource.url. By default, system CA is used
  -tlsCertFile string
     Optional path to client-side TLS certificate file to use when connecting to the -datasource.url
  -tlsInsecureSkipVerify
     Whether to skip tls verification when connecting to the -datasource.url
  -tlsKeyFile string
     Optional path to client-side TLS certificate key to use when connecting to the -datasource.url
  -tlsServerName string
     Optional TLS server name to use for connections to the -datasource.url. By default, the server name from -datasource.url is used
  -username string
     Optional basic auth username to use for the -datasource.url
  -version
     Show VictoriaMetrics version
```

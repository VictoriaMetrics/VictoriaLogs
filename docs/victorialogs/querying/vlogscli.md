---
weight:
title: vlogscli
disableToc: true
menu:
  docs:
    parent: "victorialogs-querying"
    weight: 1
tags:
  - logs
---

`vlogsqcli` is an interactive command-line tool for querying [VictoriaLogs](https://docs.victoriametrics.com/victorialogs/).
It has the following features:

- It supports scrolling and searching over query results in the same way as `less` command does - see [these docs](#scrolling-query-results).
- It supports canceling long-running queries at any time via `Ctrl+C`.
- It supports query history - see [these docs](#query-history).
- It supports different formats for query results (JSON, logfmt, compact, etc.) - see [these docs](#output-modes).
- It supports live tailing - see [these docs](#live-tailing).

This tool can be obtained from the linked release pages at the [changelog](https://docs.victoriametrics.com/victorialogs/changelog/)
or from docker images at [Docker Hub](https://hub.docker.com/r/victoriametrics/vlogscli/tags) and [Quay](https://quay.io/repository/victoriametrics/vlogscli?tab=tags).

### Running `vlogscli` from release binary

```sh
curl -L -O https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v1.25.1/vlutils-linux-amd64-v1.25.1.tar.gz
tar xzf vlutils-linux-amd64-v1.25.1.tar.gz
./vlogscli-prod
```

## Configuration

By default `vlogscli` sends queries to [`http://localhost:8429/select/logsql/query`](https://docs.victoriametrics.com/victorialogs/querying/#querying-logs).
The url to query can be changed via `-datasource.url` command-line flag. For example, the following command instructs
`vlogscli` sending queries to `https://victoria-logs.some-domain.com/select/logsql/query`:

```sh
./vlogscli -datasource.url='https://victoria-logs.some-domain.com/select/logsql/query'
```

If some HTTP request headers must be passed to the querying API, then set `-header` command-line flag.
For example, the following command starts `vlogscli`,
which queries `(AccountID=123, ProjectID=456)` [tenant](https://docs.victoriametrics.com/victorialogs/#multitenancy):

```sh
./vlogscli -header='AccountID: 123' -header='ProjectID: 456'
```

## Multitenancy

`AccountID` and `ProjectID` [values](https://docs.victoriametrics.com/victorialogs/#multitenancy)
can be set via `-accountID` and `-projectID` command-line flags:

```sh
./vlogscli -accountID=123 -projectID=456
```


## Querying

After the start `vlogscli` provides a prompt for writing [LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/) queries.
The query can be multi-line. It is sent to VictoriaLogs as soon as it contains `;` at the end or if a blank line follows the query.
For example:

```sh
;> _time:1y | count();
executing [_time:1y | stats count(*) as "count(*)"]...; duration: 0.688s
{
  "count(*)": "1923019991"
}
```

`vlogscli` shows the actually executed query on the next line after the query input prompt.
This helps debugging issues related to incorrectly written queries.

The next line after the query input prompt also shows the query duration. This helps debugging
and optimizing slow queries.

Query execution can be interrupted at any time by pressing `Ctrl+C`.

Type `q` and then press `Enter` for exit from `vlogscli` (if you want to search for `q` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word),
then just wrap it into quotes: `"q"` or `'q'`).

See also:

- [output modes](#output-modes)
- [query history](#query-history)
- [scrolling query results](#scrolling-query-results)
- [live tailing](#live-tailing)


## Scrolling query results

If the query response exceeds vertical screen space, `vlogscli` pipes query response to `less` utility,
so you can scroll the response as needed. This allows executing queries, which potentially
may return billions of rows, without any problems at both VictoriaLogs and `vlogscli` sides,
thanks to the way how `less` interacts with [`/select/logsql/query`](https://docs.victoriametrics.com/victorialogs/querying/#querying-logs):

- `less` reads the response when needed, e.g. when you scroll it down.
  `less` pauses reading the response when you stop scrolling. VictoriaLogs pauses processing the query
  when `less` stops reading the response, and automatically resumes processing the response
  when `less` continues reading it.
- `less` closes the response stream after exit from scroll mode (e.g. by typing `q`).
  VictoriaLogs stops query processing and frees up all the associated resources
  after the response stream is closed.

See also [`less` docs](https://man7.org/linux/man-pages/man1/less.1.html) and
[command-line integration docs for VictoriaLogs](https://docs.victoriametrics.com/victorialogs/querying/#command-line).


## Live tailing

`vlogscli` enters live tailing mode when the query is prepended with `\tail ` command. For example,
the following query shows all the newly ingested logs with `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word)
in real time:

```
;> \tail error;
```

By default `vlogscli` derives [the URL for live tailing](https://docs.victoriametrics.com/victorialogs/querying/#live-tailing) from the `-datasource.url` command-line flag
by replacing `/query` with `/tail` at the end of `-datasource.url`. The URL for live tailing can be specified explicitly via `-tail.url` command-line flag.

Live tailing can show query results in different formats - see [these docs](#output-modes).


## Query history

`vlogscli` supports query history - press `up` and `down` keys for navigating the history.
By default the history is stored in the `vlogscli-history` file at the directory where `vlogscli` runs,
so the history is available between `vlogscli` runs.
The path to the file can be changed via `-historyFile` command-line flag.

Quick tip: type some text and then press `Ctrl+R` for searching queries with the given text in the history.
Press `Ctrl+R` multiple times for searching other matching queries in the history.
Press `Enter` when the needed query is found in order to execute it.
Press `Ctrl+C` for exit from the `search history` mode.
See also [other available shortcuts](https://github.com/chzyer/readline/blob/f533ef1caae91a1fcc90875ff9a5a030f0237c6a/doc/shortcut.md).


## Output modes

By default `vlogscli` displays query results as prettified JSON object with every field on a separate line.
Fields in every JSON object are sorted in alphabetical order. This simplifies locating the needed fields.

`vlogscli` supports the following output modes:

* A single JSON line per every result. Type `\s` and press `enter` for this mode.
* Multiline JSON per every result. Type `\m` and press `enter` for this mode.
* Compact output. Type `\c` and press `enter` for this mode.
  This mode shows field values as is if the response contains a single field
  (for example if [`fields _msg` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#fields-pipe) is used)
  plus optional [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field).
  See also [docs about ANSI colors](#ansi-colors).
* [Logfmt output](https://brandur.org/logfmt). Type `\logfmt` and press `enter` for this mode.


## Wrapping long lines

`vlogscli` doesn't wrap long lines which do not fit screen width when it displays a response, which doesn't fit screen height.
This helps inspecting responses with many lines. If you need investigating the contents of long lines,
then press buttons with '->' and '<-' arrows on the keyboard.

Type `\wrap_long_lines` in the prompt and press enter in order to toggle automatic wrapping of long lines.

## ANSI colors

By default `vlogscli` doesn't display colored text in the compact [output mode](#output-modes) if the returned logs contain [ANSI color codes](https://en.wikipedia.org/wiki/ANSI_escape_code).
It shows the ANSI color codes instead. Type `\enable_colors` for enabling colored text. Type `\disable_color` for disabling colored text.

ANSI colors make harder analyzing the logs, so it is recommended stripping ANSI colors at data ingestion stage
according to [these docs](https://docs.victoriametrics.com/victorialogs/data-ingestion/#decolorizing).

## TLS options

`vlogscli` supports the following TLS-related command-line flags for connections to the `-datsource.url`:

* `-tlsCAFile` - optional path to TLS CA file to use for verifying connections to the `-datasource.url`. By default, system CA is used.
* `-tlsCertFile` - optional path to client-side TLS certificate file to use when connecting to the `-datasource.url`.
* `-tlsInsecureSkipVerify` - whether to skip tls verification when connecting to the `-datasource.url`.
* `-tlsKeyFile` - optional path to client-side TLS certificate key to use when connecting to the `-datasource.url`.
* `-tlsServerName` -  optional TLS server name to use for connections to the `-datasource.url`. By default, the server name from `-datasource.url` is used.

See also [auth options](#auth-options).

## Auth options

`vlogscli` supports the following auth-related command-line flags:

* `-bearerToken` - optional bearer auth token to use for the `-datasource.url`.
* `-username` - optional basic auth username to use for the `-datasource.url`.
* `-password` - optional basic auth password to use for the `-datsource.url`.

The `-bearerToken` and `-password` command-line flags may refer local files or remote files via http(s). In this case the corresponding value of the flag is read from the file.
For example, `-bearerToken=file:///abs/path/to/file`, `-bearerToken=file://./relative/path/to/file`, `-bearerToken=http://host/path` or `-bearerToken=https://host/path`.

See also [TLS options](#tls-options).

## Command-line flags

The list of command-line flags with their descriptions is available by running `./vlogscli -help`:

```
  -accountID int
    	Account ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy
  -bearerToken value
    	Optional bearer auth token to use for the -datasource.url
    	Flag value can be read from the given file when using -bearerToken=file:///abs/path/to/file or -bearerToken=file://./relative/path/to/file . Flag value can be read from the given http/https url when using -bearerToken=http://host/path or -bearerToken=https://host/path
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
  -header array
    	Optional header to pass in request -datasource.url in the form 'HeaderName: value'
    	Supports an array of values separated by comma or specified via multiple flags.
    	Value can contain comma inside single-quoted or double-quoted string, {}, [] and () braces.
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
    	Optional basic auth password to use for the -datsource.url
    	Flag value can be read from the given file when using -password=file:///abs/path/to/file or -password=file://./relative/path/to/file . Flag value can be read from the given http/https url when using -password=http://host/path or -password=https://host/path
  -projectID int
    	Project ID to query; see https://docs.victoriametrics.com/victorialogs/#multitenancy
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

### Building from source code

Follow these steps in order to build `vlogscli` from source code:

- Checkout VictoriaLogs source code:

  ```sh
  git clone https://github.com/VictoriaMetrics/VictoriaLogs
  cd VictoriaLogs
  ```

- Build `vlogscli`:

  ```sh
  make vlogscli
  ```

- Run the built binary:

  ```sh
  bin/vlogscli -datasource.url=http://victoria-logs-host:9428/select/logsql/query
  ```

Replace `victoria-los-host:9428` with the needed hostname of the VictoriaLogs to query.

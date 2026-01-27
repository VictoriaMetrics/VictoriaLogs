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

- It supports scrolling and searching over query results in the same way as `less` command does - see [these docs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#scrolling-query-results).
- It supports canceling long-running queries at any time via `Ctrl+C`.
- It supports query history - see [these docs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#query-history).
- It supports different formats for query results (JSON, logfmt, compact, etc.) - see [these docs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#output-modes).
- It supports live tailing - see [these docs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#live-tailing).

This tool can be obtained from the linked release pages at the [changelog](https://docs.victoriametrics.com/victorialogs/changelog/)
or from docker images at [Docker Hub](https://hub.docker.com/r/victoriametrics/vlogscli/tags) and [Quay](https://quay.io/repository/victoriametrics/vlogscli?tab=tags).

### Running `vlogscli` from release binary

```sh
curl -L -O https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v1.44.0/vlutils-linux-amd64-v1.44.0.tar.gz
tar xzf vlutils-linux-amd64-v1.44.0.tar.gz
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
executing [_time:1y | stats count(*) as "count(*)"]...; server duration: 0.688s
{
  "count(*)": "1923019991"
}
```

`vlogscli` shows the actually executed query on the next line after the query input prompt.
This helps debugging issues related to incorrectly written queries.

The next line after the query input prompt also shows the query duration. It shows the duration
the query took to execute until the first response byte. It doesn't include the time needed to send the request to the server and receive the response from the server.
This helps debugging and optimizing slow queries.

Query execution can be interrupted at any time by pressing `Ctrl+C`.

Type `q` and then press `Enter` for exit from `vlogscli` (if you want to search for `q` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word),
then just wrap it into quotes: `"q"` or `'q'`).

See also:

- [output modes](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#output-modes)
- [query history](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#query-history)
- [scrolling query results](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#scrolling-query-results)
- [live tailing](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#live-tailing)

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

`vlogscli` enters live tailing mode when the query is prepended with `\tail` command. For example,
the following query shows all the newly ingested logs with `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word)
in real time:

```
;> \tail error;
```

By default `vlogscli` derives [the URL for live tailing](https://docs.victoriametrics.com/victorialogs/querying/#live-tailing) from the `-datasource.url` command-line flag
by replacing `/query` with `/tail` at the end of `-datasource.url`. The URL for live tailing can be specified explicitly via `-tail.url` command-line flag.

Live tailing can show query results in different formats - see [these docs](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#output-modes).

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

- A single JSON line per every result. Type `\s` and press `enter` for this mode.
- Multiline JSON per every result. Type `\m` and press `enter` for this mode.
- Compact output. Type `\c` and press `enter` for this mode.
  This mode shows field values as is if the response contains a single field
  (for example if [`fields _msg` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#fields-pipe) is used)
  plus optional [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field).
  See also [docs about ANSI colors](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#ansi-colors).
- [Logfmt output](https://brandur.org/logfmt). Type `\logfmt` and press `enter` for this mode.

## Wrapping long lines

`vlogscli` doesn't wrap long lines which do not fit screen width when it displays a response, which doesn't fit screen height.
This helps inspecting responses with many lines. If you need investigating the contents of long lines,
then press buttons with '->' and '<-' arrows on the keyboard.

Type `\wrap_long_lines` in the prompt and press enter in order to toggle automatic wrapping of long lines.

## ANSI colors

By default `vlogscli` doesn't display colored text in the compact [output mode](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#output-modes) if the returned logs contain [ANSI color codes](https://en.wikipedia.org/wiki/ANSI_escape_code).
It shows the ANSI color codes instead. Type `\enable_colors` for enabling colored text. Type `\disable_color` for disabling colored text.

ANSI colors make harder analyzing the logs, so it is recommended stripping ANSI colors at data ingestion stage
according to [these docs](https://docs.victoriametrics.com/victorialogs/data-ingestion/#decolorizing).

## TLS options

`vlogscli` supports the following TLS-related command-line flags for connections to the `-datasource.url`:

- `-tlsCAFile` - optional path to TLS CA file to use for verifying connections to the `-datasource.url`. By default, system CA is used.
- `-tlsCertFile` - optional path to client-side TLS certificate file to use when connecting to the `-datasource.url`.
- `-tlsInsecureSkipVerify` - whether to skip tls verification when connecting to the `-datasource.url`.
- `-tlsKeyFile` - optional path to client-side TLS certificate key to use when connecting to the `-datasource.url`.
- `-tlsServerName` -  optional TLS server name to use for connections to the `-datasource.url`. By default, the server name from `-datasource.url` is used.

See also [auth options](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#auth-options).

## Auth options

`vlogscli` supports the following auth-related command-line flags:

- `-bearerToken` - optional bearer auth token to use for the `-datasource.url`.
- `-username` - optional basic auth username to use for the `-datasource.url`.
- `-password` - optional basic auth password to use for the `-datasource.url`.

The `-bearerToken` and `-password` command-line flags may refer local files or remote files via http(s). In this case the corresponding value of the flag is read from the file.
For example, `-bearerToken=file:///abs/path/to/file`, `-bearerToken=file://./relative/path/to/file`, `-bearerToken=http://host/path` or `-bearerToken=https://host/path`.

See also [TLS options](https://docs.victoriametrics.com/victorialogs/querying/vlogscli/#tls-options).

## Command-line flags

The list of command-line flags with their descriptions is available by running `./vlogscli -help`:

{{% content "vlogscli_common_flags.md" %}}

### Building from source code

Follow these steps in order to build `vlogscli` from source code:

- Checkout VictoriaLogs source code:

  ```sh
  git clone https://github.com/VictoriaMetrics/VictoriaLogs
  cd VictoriaLogs
  ```

- Checkout to the needed commit if needed:

  ```sh
  git checkout <commit-hash-here>
  ```

- Build `vlogscli`. It requires Go installed on your computer. See [how to install Go](https://golang.org/doc/install):

  ```sh
  make vlogscli
  ```

- Run the built binary:

  ```sh
  bin/vlogscli -datasource.url=http://victoria-logs-host:9428/select/logsql/query
  ```

Replace `victoria-los-host:9428` with the needed hostname of the VictoriaLogs to query.

An alternative approach is to build `vlogscli` inside Docker builder container. This approach doesn't require Go installation,
but it requires Docker installed on your computer. See [how to install Docker](https://docs.docker.com/engine/install/):

```sh
make vlogscli-prod
```

This will build `vlogscli-prod` executable inside the `bin` folder.

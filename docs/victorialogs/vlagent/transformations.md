---
weight: 1
title: Transformations
menu:
  docs:
    parent: "vlagent"
    weight: 1
tags:
  - logs
aliases:
  - /victorialogs/vlagent/transformations/
  - /victorialogs/vlagent/transformations.html
---

`vlagent` can transform logs before sending them to remote storage.
Use it to normalize fields, parse unstructured messages, enrich records, and route logs based on their content.

Transformations are written in the VictoriaLogs transformations language, which is built on top of [LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/).
LogsQL queries data that is already in storage, while transformations process logs line-by-line before they reach remote storage.

The transformation language adds control flow on top of LogsQL.
You can declare reusable transformation blocks, process logs conditionally, and drop unwanted data independently for each remote storage destination.
You can also override the [`_stream`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) field or change the target tenant based on log content.

## Quick start

To start transforming logs, pass the `-remoteWrite.transforms` flag with one
or more [supported](https://docs.victoriametrics.com/victorialogs/vlagent/transformations/#supported-pipes) LogsQL pipes.
For example:

```sh
./vlagent -remoteWrite.url=http://victorialogs:9428/insert/native \
  -remoteWrite.transforms="inline: extract 'my service name is: <service>' from _msg | set_stream_fields service;"
```

This extracts the service name from the `_msg` field and uses it as the [`_stream`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) field.

An incoming log like this:

```json
{
  "_msg": "my service name is: payment-api"
}
```

is sent to VictoriaLogs as:

```json
{
  "_msg": "my service name is payment-api",
  "_stream": "{service=\"payment-api\"}",
  "_time": "...",
  "service": "payment-api"
}
```

To load transformations from a file instead, pass its path to `-remoteWrite.transforms`.
For example, create `/tmp/transforms.vlt` with the following content:

```vlt
coalesce(level, log.level, LEVEL) as level;

if (level:"") {
  # The 'level' field is empty or not set - try to analyze the message content.
  if (_msg:i(error)) {
    # '_msg' field contains the word 'error'
    format error as level;
  } else {
    format unknown as level;
  }
}
```

Then run `vlagent` with the path to the file:

```sh
./vlagent -remoteWrite.url=http://victorialogs:9428/insert/native \
  -remoteWrite.transforms="/tmp/transforms.vlt"
```

`vlagent` sets the `level` field for each log based on its content.
First it takes the first non-empty value among `level`, `log.level`, and `LEVEL` and writes it to `level`.
Then, if `level` is still empty and the message contains the word `error`, it sets `level` to `error`, otherwise it sets it to `unknown`.

For more detail, see the [syntax description](https://docs.victoriametrics.com/victorialogs/vlagent/transformations/#syntax)
and ready-to-use [log transformation examples](https://docs.victoriametrics.com/victorialogs/vlagent/transformations/#examples).

## Syntax

A transformation program is a sequence of statements, one per line.
A statement can be:

* a pipes line: one or more LogsQL pipes connected via `|`.
* an `if` condition.
* a `block` declaration or its invocation via `do`.
* a control statement: `return`, `send`, or `drop`.

A pipes line, `return`, `send`, and `drop` must end with `;`.

Each statement is described below.

### Pipes line

Multiple pipes in a single line are applied left to right:

```vlt
unpack_json | rename log.level as level | delete _msg | rename message as _msg;
```

This is equivalent to a line-by-line definition:

```vlt
unpack_json;
rename log.level as level;
delete _msg;
rename message as _msg;
```

Any supported [LogsQL](https://docs.victoriametrics.com/victorialogs/vlagent/transformations/#supported-pipes) pipe can be used here.

### Conditional execution

`if` applies nested statements only to logs that match the filter.
The filter can be any [LogsQL filter](https://docs.victoriametrics.com/victorialogs/logsql/#filters):
an [exact filter](https://docs.victoriametrics.com/victorialogs/logsql/#exact-filter),
a [word match filter](https://docs.victoriametrics.com/victorialogs/logsql/#word-filter),
a [prefix filter](https://docs.victoriametrics.com/victorialogs/logsql/#prefix-filter),
an [IP filter](https://docs.victoriametrics.com/victorialogs/logsql/#ipv4-range-filter), and others.
Logs that do not match pass through unchanged.

```vlt
if (level:=error) {
  unpack_json;
}
```

`else if` chains and a trailing `else` are supported:

```vlt
if (level:=error) {
  format error as kind;
} else if (level:=warn) {
  format warning as kind;
} else {
  format other as kind;
}
```

### Named blocks

A reusable set of statements can be extracted into a named block and invoked with `do`:

```vlt
block normalize {
  unpack_json;
  rename level as severity;
}

do normalize;
```

Blocks are declared at the top level of the program.
A block cannot be declared inside another block or inside an `if` statement.
Recursive block invocations are forbidden, even when executed conditionally.

A block is only visible within the file where it is declared.
You cannot invoke a block declared in another file.

### Control statements

#### `send`

`send` sends the current log downstream - to the per-URL transformations (if any)
and then to each `-remoteWrite.url` - and stops further processing by the remaining statements.
Use it to handle transformations separately for each service:

```vlt
if (service:=payment) {
  unpack_logfmt;
  rename MESSAGE as _msg;
  rename log.level as level;
  send;
}
if (service:=checkout) {
  unpack_json;
  send;
}

format 'unknown service' as transforms_status;
```

In the program above, logs from the `payment` and `checkout` services are sent downstream right away
and never reach the `format 'unknown service' as transforms_status` statement.
You can call `send` inside a `block` too, where it works the same way.

#### `return`

`return` stops the current block and returns log processing to the statements following `do`.
Use it for an early exit from a block:

```vlt
block normalize_errors {
  if (not level:=error) {
    return;
  }
  format critical as severity;
}

do normalize_errors;
format processed as status;
```

A log whose `level` is not `error` exits the `normalize_errors` block right away,
so the `format critical as severity` pipe is not applied to it.
Every log still reaches the `format processed as status` pipe, regardless of its `level` field.

#### `drop`

`drop` discards the current log, and subsequent pipes do not process it.
For example, to ignore all logs with the `debug` level:

```vlt
if (level:=debug) {
  # Drop all debug logs.
  drop;
}
```

## Supported pipes

VictoriaLogs transformations support a subset of LogsQL pipes.
Aggregating pipes such as `stats`, `sort`, `top`, `uniq`, `last`, and `facets` are not supported, because each log is processed on its own.
The same goes for pipes like `limit`, `offset`, `first`, and `filter`, which only make sense inside a query.

The supported pipes are listed below:

| Pipe                  | Purpose                                      | Documentation                                                                                     |
|-----------------------|----------------------------------------------|---------------------------------------------------------------------------------------------------|
| `coalesce`            | first non-empty value from a list of fields  | [coalesce](https://docs.victoriametrics.com/victorialogs/logsql/#coalesce-pipe)                   |
| `collapse_nums`       | collapsing numeric sequences into a template | [collapse_nums](https://docs.victoriametrics.com/victorialogs/logsql/#collapse_nums-pipe)         |
| `copy`, `cp`          | copying fields                               | [copy](https://docs.victoriametrics.com/victorialogs/logsql/#copy-pipe)                           |
| `decolorize`          | removing ANSI color codes                    | [decolorize](https://docs.victoriametrics.com/victorialogs/logsql/#decolorize-pipe)               |
| `delete`, `del`, `rm` | deleting fields                              | [delete](https://docs.victoriametrics.com/victorialogs/logsql/#delete-pipe)                       |
| `drop_empty_fields`   | removing empty fields                        | [drop_empty_fields](https://docs.victoriametrics.com/victorialogs/logsql/#drop_empty_fields-pipe) |
| `extract`             | extracting fields by template                | [extract](https://docs.victoriametrics.com/victorialogs/logsql/#extract-pipe)                     |
| `extract_regexp`      | extracting fields by regular expression      | [extract_regexp](https://docs.victoriametrics.com/victorialogs/logsql/#extract_regexp-pipe)       |
| `math`, `eval`        | arithmetic calculations on fields            | [math](https://docs.victoriametrics.com/victorialogs/logsql/#math-pipe)                           |
| `fields`, `keep`      | keep only specified fields                   | [fields](https://docs.victoriametrics.com/victorialogs/logsql/#fields-pipe)                       |
| `format`              | formatting a field by template               | [format](https://docs.victoriametrics.com/victorialogs/logsql/#format-pipe)                       |
| `hash`                | hashing field values                         | [hash](https://docs.victoriametrics.com/victorialogs/logsql/#hash-pipe)                           |
| `json_array_len`      | JSON array length in a field                 | [json_array_len](https://docs.victoriametrics.com/victorialogs/logsql/#json_array_len-pipe)       |
| `len`                 | field value length                           | [len](https://docs.victoriametrics.com/victorialogs/logsql/#len-pipe)                             |
| `rename`, `mv`        | renaming fields                              | [rename](https://docs.victoriametrics.com/victorialogs/logsql/#rename-pipe)                       |
| `pack_json`           | packing fields into JSON                     | [pack_json](https://docs.victoriametrics.com/victorialogs/logsql/#pack_json-pipe)                 |
| `pack_logfmt`         | packing fields into logfmt                   | [pack_logfmt](https://docs.victoriametrics.com/victorialogs/logsql/#pack_logfmt-pipe)             |
| `replace`             | replacing substrings in fields               | [replace](https://docs.victoriametrics.com/victorialogs/logsql/#replace-pipe)                     |
| `replace_regexp`      | replacing by regular expression              | [replace_regexp](https://docs.victoriametrics.com/victorialogs/logsql/#replace_regexp-pipe)       |
| `sample`              | log stream downsampling (sampling)           | [sample](https://docs.victoriametrics.com/victorialogs/logsql/#sample-pipe)                       |
| `set_stream_fields`   | overriding the `_stream` field               | [set_stream_fields](https://docs.victoriametrics.com/victorialogs/logsql/#set_stream_fields-pipe) |
| `split`               | splitting a field value                      | [split](https://docs.victoriametrics.com/victorialogs/logsql/#split-pipe)                         |
| `time_add`            | shifting log time                            | [time_add](https://docs.victoriametrics.com/victorialogs/logsql/#time_add-pipe)                   |
| `unpack_json`         | parsing JSON into fields                     | [unpack_json](https://docs.victoriametrics.com/victorialogs/logsql/#unpack_json-pipe)             |
| `unpack_logfmt`       | parsing logfmt into fields                   | [unpack_logfmt](https://docs.victoriametrics.com/victorialogs/logsql/#unpack_logfmt-pipe)         |
| `unpack_syslog`       | parsing syslog into fields                   | [unpack_syslog](https://docs.victoriametrics.com/victorialogs/logsql/#unpack_syslog-pipe)         |
| `unpack_words`        | parsing a value into words                   | [unpack_words](https://docs.victoriametrics.com/victorialogs/logsql/#unpack_words-pipe)           |
| `unroll`              | unrolling an array into separate logs        | [unroll](https://docs.victoriametrics.com/victorialogs/logsql/#unroll-pipe)                       |

## Configuration

`-remoteWrite.transforms` applies global transformations to all logs before they are sent to every `-remoteWrite.url`:

```sh
./vlagent -remoteWrite.transforms=transforms.vlt
```

`-remoteWrite.urlTransforms` applies transformations to a single `-remoteWrite.url`.
The flag is matched to `-remoteWrite.url` by position.
The first `-remoteWrite.urlTransforms` goes with the first `-remoteWrite.url`, the second with the second, and so on:

```sh
./vlagent \
  -remoteWrite.transforms=global.vlt \
  -remoteWrite.url=http://victoria-logs-hot:9428/insert/native \
  -remoteWrite.urlTransforms=transforms-hot.vlt \
  -remoteWrite.url=http://victoria-logs-cold:9428/insert/native \
  -remoteWrite.urlTransforms=transforms-cold.vlt
```

To skip per-URL transformations for a specific `-remoteWrite.url`, pass an empty value: `-remoteWrite.urlTransforms=""`.

When both global and per-URL transformations are set, the global one runs first,
and its result is passed to the per-URL transformations of each matching `-remoteWrite.url`.

### Transformation sources

The value of `-remoteWrite.transforms` and `-remoteWrite.urlTransforms` can be one of the following:

- A path to a single file, for example `-remoteWrite.transforms=/etc/vlagent/transforms.vlt`.
- A glob pattern matching one or more files, for example `-remoteWrite.transforms=/etc/vlagent/*.vlt`.
  Matched files are sorted lexicographically and applied in that order.
  For example, files `01.vlt`, `03.vlt`, `02.vlt` are applied as `01.vlt`, `02.vlt`, `03.vlt`.
- An `http` or `https` URL, for example `-remoteWrite.transforms=http://config-server/transforms.vlt`.
- An inline program prefixed with `inline:`, for example `-remoteWrite.transforms="inline: unpack_json | delete _msg;"`.

### Environment variables substitution

A transformation program can reference environment variables with the `%{VAR_NAME}` syntax.
Values are substituted into the program text as-is, without escaping, so they can include transformations language syntax.

For example:

```vlt
if (service:in(%{IGNORED_SERVICES})) {
  drop;
}
```

With this environment variable:

```sh
IGNORED_SERVICES="payment,checkout"
```

the program expands into:

```vlt
if (service:in(payment,checkout)) {
  drop;
}
```

Environment variables cannot be changed while `vlagent` is running - restart `vlagent` to apply changes.

See [these docs](https://docs.victoriametrics.com/victoriametrics/#environment-variables) for more details.

## Examples

### Dynamic setting of the `_stream` field

```vlt
unpack_logfmt from _msg;
set_stream_fields service, level, host;
```

### Parsing unstructured logs

Suppose an application writes logs in this format:

```json
{
  "_msg": "User login successful for john.doe@example.com at 2025-05-30T15:10:22Z"
}
```

To turn it into a structured record, use the `extract` pipe:

```vlt
extract "User login successful for <email> at <login_timestamp>" from _msg;

if (email:* and login_timestamp:*) {
  # Content was extracted - override the _msg field.
  format "User login successful" as _msg;
}
```

This produces the following log:

```json
{
  "_msg": "User login successful",
  "email": "john.doe@example.com",
  "login_timestamp": "2025-05-30T15:10:22Z"
}
```

### Log level normalization

The program below stores the log level in a single `level` field.
If the level is missing, it infers it from the log message content.

```vlt
do normalize_log_level;

block normalize_log_level {
  coalesce(level, LEVEL, lvl, log.level, severity, severity_text, SeverityText) as level;
  delete LEVEL, lvl, log.level, severity, severity_text, SeverityText;

  if (level:"") {
    # The 'level' field is empty or not set - try to analyze the message content.
    if (i(error)) {
      # '_msg' field contains the word 'error'
      format error as level;
    } else {
      format unknown as level;
    }
  }
}
```

### Routing logs per destination

Each `-remoteWrite.urlTransforms` filters its destination independently,
so you can split logs between storages by dropping the unwanted ones per URL.

For example, send audit logs to a dedicated `audit` storage and everything else to the main `hot` storage.
The two programs are mirror images of each other:

```vlt
# hot.vlt - everything except audit logs
if (event_type:=audit) {
  drop;
}
```

```vlt
# audit.vlt - audit logs only
if (not event_type:=audit) {
  drop;
}
```

```sh
./vlagent \
  -remoteWrite.url=http://victorialogs-hot:9428/insert/native \
  -remoteWrite.urlTransforms="hot.vlt" \
  -remoteWrite.url=http://victorialogs-audit:9428/insert/native \
  -remoteWrite.urlTransforms="audit.vlt"
```

`hot` drops audit logs and keeps the rest, while `audit` drops everything that is not an audit log.
Because the two filters are opposite, each log lands in exactly one storage.

### Dropping logs for a single destination

You can send the same logs to several storages and drop only a part of them for one destination.
This is useful when one storage does not need the full volume.

For example, keep all logs in `hot`, but skip debug logs in `cold` to save space:

```vlt
# cold.vlt
if (level:=debug) {
  drop;
}
```

```sh
./vlagent \
  -remoteWrite.url=http://victorialogs-hot:9428/insert/native \
  -remoteWrite.urlTransforms="" \
  -remoteWrite.url=http://victorialogs-cold:9428/insert/native \
  -remoteWrite.urlTransforms="cold.vlt"
```

`hot` has no per-URL program, so it gets every log, including debug ones.
`cold` drops debug logs and keeps everything else.
A non-debug log goes to both storages - unlike [logs routing](https://docs.victoriametrics.com/victorialogs/vlagent/transformations/#routing-logs-per-destination), the destinations overlap.

### Dynamic tenant assignment

During transformations, `vlagent` provides the `vl_account_id` and `vl_project_id` fields.
They set the [tenant](https://docs.victoriametrics.com/victorialogs/#multitenancy) that the log is written to in VictoriaLogs.

To set the tenant dynamically, use the `/insert/multitenant/native` endpoint instead of `/insert/native`,
as described in the [vlagent multitenancy](https://docs.victoriametrics.com/victorialogs/vlagent/#multitenancy) docs.

Override `vl_account_id` and `vl_project_id` based on log content to change the target tenant.
For example:

```vlt
if (kubernetes.pod_namespace:=kube-system) {
  format 1 as vl_account_id;
  format 0 as vl_project_id;
}
if (kubernetes.pod_namespace:=prod) {
  format 2 as vl_account_id;
  format 0 as vl_project_id;
}
if (kubernetes.pod_namespace:=stg) {
  format 3 as vl_account_id;
  format 0 as vl_project_id;
}
# ...
```

The tenant value can be read directly from the log content.
To do this, produce the `vl_account_id` or `vl_project_id` field in a transformation with `rename`, `format`,
or any pipe that assigns a value to the field:

```vlt
if (kubernetes.pod_annotations.account_id:* and kubernetes.pod_annotations.project_id:*) {
  # both 'account_id' and 'project_id' annotations are set, use them to override the target tenant.
  rename kubernetes.pod_annotations.account_id as vl_account_id;
  rename kubernetes.pod_annotations.project_id as vl_project_id;
}
```

This sets the tenant from the `kubernetes.pod_annotations.account_id` and `kubernetes.pod_annotations.project_id` fields,
so the target tenant is controlled through Kubernetes annotations.

If `vl_account_id` and `vl_project_id` fields arrive in the incoming log,
`vlagent` treats them as ordinary fields and does not change the tenant.

### Head sampling based on trace_id

The program below keeps only 20% of logs for the `payments` service, based on the `trace_id` field.

```vlt
if (service:=payments) {
  do sample_by_trace_id;
}

block sample_by_trace_id {
  if (trace_id:"") {
    # The 'trace_id' field is not set.
    return;
  }

  # The 'hash_remainder' field will contain a hash remainder from 0 to 99.
  hash(trace_id) as trace_id_hash | math trace_id_hash % 100 as hash_remainder;

  # Keep remainders 0-19 (20%), drop the rest.
  if (hash_remainder:>=20) {
    drop;
  }
  delete trace_id_hash, hash_remainder;
}
```

Sampling is applied per log, so it gives consistent results even across nodes that share the same `vlagent` configuration.

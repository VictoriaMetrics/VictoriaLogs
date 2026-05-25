---
weight: 50
title: LogsQL Examples
menu:
  docs:
    parent: "victorialogs"
    weight: 50
tags:
  - logs
  - guide
---

## How to select recently ingested logs?

[Run](https://docs.victoriametrics.com/victorialogs/querying/) the following query:

```logsql
_time:5m
```

It returns logs over the last 5 minutes by using [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter).
The logs are returned in arbitrary order because of performance reasons.
Add [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe) to the query if you need sorting
the returned logs by some field (usually [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field)):

```logsql
_time:5m | sort by (_time)
```

If the number of returned logs is too big, it may be limited with the [`first` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#first-pipe).
For example, the following query returns 10 most recent logs, which were ingested during the last 5 minutes:

```logsql
_time:5m | first 10 by (_time desc)
```

See also:

- [How to count the number of matching logs?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-count-the-number-of-matching-logs)
- [How to return last N logs for the given query?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-return-last-n-logs-for-the-given-query)

## How to select logs with the given word in log message?

Just put the needed [word](https://docs.victoriametrics.com/victorialogs/logsql/#word) in the query.
For example, the following query returns all the logs with the `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word)
in [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):

```logsql
error
```

If the number of returned logs is too big, then add [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter)
for limiting the time range for the selected logs. For example, the following query returns logs with `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word)
over the last hour:

```logsql
error _time:1h
```

If the number of returned logs is still too big, then consider adding more specific [filters](https://docs.victoriametrics.com/victorialogs/logsql/#filters)
to the query. For example, the following query selects logs with `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word),
which do not contain `kubernetes` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word), over the last hour:

```logsql
error -kubernetes _time:1h
```

The logs are returned in arbitrary order because of performance reasons. Add [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe)
for sorting logs by the needed [fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model). For example, the following query
sorts the selected logs by [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field):

```logsql
error _time:1h | sort by (_time)
```

See also:

- [How to select logs with all the given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-all-the-given-words-in-log-message)
- [How to select logs with some of the given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-some-of-the-given-words-in-log-message)
- [How to skip logs with the given word in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-skip-logs-with-the-given-word-in-log-message)
- [Filtering by phrase](https://docs.victoriametrics.com/victorialogs/logsql/#phrase-filter)
- [Filtering by prefix](https://docs.victoriametrics.com/victorialogs/logsql/#prefix-filter)
- [Filtering by regular expression](https://docs.victoriametrics.com/victorialogs/logsql/#regexp-filter)
- [Filtering by substring](https://docs.victoriametrics.com/victorialogs/logsql/#substring-filter)

## How to skip logs with the given word in log message?

Use [`NOT` logical filter](https://docs.victoriametrics.com/victorialogs/logsql/#logical-filter). For example, the following query returns all the logs
without the `INFO` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word) in the [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):

```logsql
-INFO
```

If the number of returned logs is too big, then add [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter)
for limiting the time range for the selected logs. For example, the following query returns matching logs over the last hour:

```logsql
-INFO _time:1h
```

If the number of returned logs is still too big, then consider adding more specific [filters](https://docs.victoriametrics.com/victorialogs/logsql/#filters)
to the query. For example, the following query selects logs without `INFO` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word),
which contain `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word), over the last hour:

```logsql
-INFO error _time:1h
```

The logs are returned in arbitrary order because of performance reasons. Add [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe)
for sorting logs by the needed [fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model). For example, the following query
sorts the selected logs by [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field):

```logsql
-INFO _time:1h | sort by (_time)
```

See also:

- [How to select logs with all the given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-all-the-given-words-in-log-message)
- [How to select logs with some of given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-some-of-the-given-words-in-log-message)
- [Filtering by phrase](https://docs.victoriametrics.com/victorialogs/logsql/#phrase-filter)
- [Filtering by prefix](https://docs.victoriametrics.com/victorialogs/logsql/#prefix-filter)
- [Filtering by regular expression](https://docs.victoriametrics.com/victorialogs/logsql/#regexp-filter)
- [Filtering by substring](https://docs.victoriametrics.com/victorialogs/logsql/#substring-filter)

## How to select logs with all the given words in log message?

Just enumerate the needed [words](https://docs.victoriametrics.com/victorialogs/logsql/#word) in the query, by delimiting them with whitespace.
For example, the following query selects logs containing both `error` and `kubernetes` [words](https://docs.victoriametrics.com/victorialogs/logsql/#word)
in the [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):

```logsql
error kubernetes
```

This query uses [`AND` logical filter](https://docs.victoriametrics.com/victorialogs/logsql/#logical-filter).

If the number of returned logs is too big, then add [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter)
for limiting the time range for the selected logs. For example, the following query returns matching logs over the last hour:

```logsql
error kubernetes _time:1h
```

If the number of returned logs is still too big, then consider adding more specific [filters](https://docs.victoriametrics.com/victorialogs/logsql/#filters)
to the query. For example, the following query selects logs with `error` and `kubernetes` [words](https://docs.victoriametrics.com/victorialogs/logsql/#word)
from [log streams](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) containing `container="my-app"` field, over the last hour:

```logsql
error kubernetes {container="my-app"} _time:1h
```

The logs are returned in arbitrary order because of performance reasons. Add [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe)
for sorting logs by the needed [fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model). For example, the following query
sorts the selected logs by [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field):

```logsql
error kubernetes _time:1h | sort by (_time)
```

See also:

- [How to select logs with some of given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-some-of-the-given-words-in-log-message)
- [How to skip logs with the given word in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-skip-logs-with-the-given-word-in-log-message)
- [Filtering by phrase](https://docs.victoriametrics.com/victorialogs/logsql/#phrase-filter)
- [Filtering by prefix](https://docs.victoriametrics.com/victorialogs/logsql/#prefix-filter)
- [Filtering by regular expression](https://docs.victoriametrics.com/victorialogs/logsql/#regexp-filter)
- [Filtering by substring](https://docs.victoriametrics.com/victorialogs/logsql/#substring-filter)

## How to select logs with some of the given words in log message?

Put the needed [words](https://docs.victoriametrics.com/victorialogs/logsql/#word) into `(...)`, by delimiting them with ` or `.
For example, the following query selects logs with `error`, `ERROR` or `Error` [words](https://docs.victoriametrics.com/victorialogs/logsql/#word)
in the [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):

```logsql
(error or ERROR or Error)
```

This query uses [`OR` logical filter](https://docs.victoriametrics.com/victorialogs/logsql/#logical-filter).

If the number of returned logs is too big, then add [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter)
for limiting the time range for the selected logs. For example, the following query returns matching logs over the last hour:

```logsql
(error or ERROR or Error) _time:1h
```

If the number of returned logs is still too big, then consider adding more specific [filters](https://docs.victoriametrics.com/victorialogs/logsql/#filters)
to the query. For example, the following query selects logs without `error`, `ERROR` or `Error` [words](https://docs.victoriametrics.com/victorialogs/logsql/#word),
which do not contain `kubernetes` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word), over the last hour:

```logsql
(error or ERROR or Error) -kubernetes _time:1h
```

The logs are returned in arbitrary order because of performance reasons. Add [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe)
for sorting logs by the needed [fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model). For example, the following query
sorts the selected logs by [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field):

```logsql
(error or ERROR or Error) _time:1h | sort by (_time)
```

See also:

- [How to select logs with all the given words in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-with-all-the-given-words-in-log-message)
- [How to skip logs with the given word in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-skip-logs-with-the-given-word-in-log-message)
- [Filtering by phrase](https://docs.victoriametrics.com/victorialogs/logsql/#phrase-filter)
- [Filtering by prefix](https://docs.victoriametrics.com/victorialogs/logsql/#prefix-filter)
- [Filtering by regular expression](https://docs.victoriametrics.com/victorialogs/logsql/#regexp-filter)
- [Filtering by substring](https://docs.victoriametrics.com/victorialogs/logsql/#substring-filter)

## How to select logs from the given application instance?

Make sure the application is properly configured with [stream-level log fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields).
Then just use [`_stream` filter](https://docs.victoriametrics.com/victorialogs/logsql/#stream-filter) for selecting logs for the given application instance.
For example, if the application contains `job="app-42"` and `instance="host-123:5678"` [stream fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields),
then the following query selects all the logs from this application:

```logsql
{job="app-42",instance="host-123:5678"}
```

If the number of returned logs is too big, it is recommended adding [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter)
to the query in order to reduce the number of matching logs. For example, the following query returns logs for the given application for the last day:

```logsql
{job="app-42",instance="host-123:5678"} _time:1d
```

If the number of returned logs is still too big, then consider adding more specific [filters](https://docs.victoriametrics.com/victorialogs/logsql/#filters)
to the query. For example, the following query selects logs from the given [log stream](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields),
which contain `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word) in the [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field),
over the last day:

```logsql
{job="app-42",instance="host-123:5678"} error _time:1d
```

The logs are returned in arbitrary order because of performance reasons. Use [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe)
for sorting the returned logs by the needed fields. For example, the following query sorts the selected logs
by [`_time`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field):

```logsql
{job="app-42",instance="host-123:5678"} _time:1d | sort by (_time)
```

See also:

- [How to determine applications with the most logs?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-determine-applications-with-the-most-logs)
- [How to skip logs with the given word in log message?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-skip-logs-with-the-given-word-in-log-message)

## How to count the number of matching logs?

Use [`count()` stats function](https://docs.victoriametrics.com/victorialogs/logsql/#count-stats). For example, the following query returns
the number of results returned by `your_query_here`:

```logsql
your_query_here | count()
```

## How to determine applications with the most logs?

[Run](https://docs.victoriametrics.com/victorialogs/querying/) the following query:

```logsql
_time:5m | stats by (_stream) count() as logs | sort by (logs desc) | limit 10
```

This query returns top 10 application instances (aka [log streams](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields))
with the most logs over the last 5 minutes.

This query uses the following [LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/) features:

- [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter) for selecting logs on the given time range (5 minutes in the query above).
- [`stats` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#stats-pipe) for calculating the number of logs.
  per each [`_stream`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields). [`count` stats function](https://docs.victoriametrics.com/victorialogs/logsql/#count-stats)
  is used for calculating the needed stats.
- [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe) for sorting the stats by `logs` field in descending order.
- [`limit` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#limit-pipe) for limiting the number of returned results to 10.

This query can be simplified into the following one, which uses [`top` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#top-pipe):

```logsql
_time:5m | top 10 by (_stream)
```

See also:

- [How to filter out data after stats calculation?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-filter-out-data-after-stats-calculation)
- [How to calculate the number of logs per the given interval?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-calculate-the-number-of-logs-per-the-given-interval)
- [How to select logs from the given application instance?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-logs-from-the-given-application-instance)

## How to parse JSON inside log message?

It is better from performance and resource usage PoV to avoid storing JSON inside [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field).
It is recommended storing individual JSON fields as log fields instead according to [VictoriaLogs data model](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model).

If you have to store JSON inside log message or inside any other [log fields](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model),
then the stored JSON can be parsed during query time via [`unpack_json` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#unpack_json-pipe).
For example, the following query unpacks JSON from the [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field)
across all the logs for the last 5 minutes:

```logsql
_time:5m | unpack_json
```

If you need to parse JSON array, then take a look at [`unroll` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#unroll-pipe).

## How to extract some data from text log message?

Use [`extract`](https://docs.victoriametrics.com/victorialogs/logsql/#extract-pipe) or [`extract_regexp`](https://docs.victoriametrics.com/victorialogs/logsql/#extract_regexp-pipe) pipe.
For example, the following query extracts `username` and `user_id` fields from text [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field):

```logsql
_time:5m | extract "username=<username>, user_id=<user_id>,"
```

See also:

- [Replacing substrings in text fields](https://docs.victoriametrics.com/victorialogs/logsql/#replace-pipe)

## How to filter out data after stats calculation?

Use [`filter` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#filter-pipe). For example, the following query
returns only [log streams](https://docs.victoriametrics.com/victorialogs/keyconcepts/#stream-fields) with more than 1000 logs
over the last 5 minutes:

```logsql
_time:5m | stats by (_stream) count() rows | filter rows:>1000
```

## How to calculate the number of logs per the given interval?

Use [`stats` by time bucket](https://docs.victoriametrics.com/victorialogs/logsql/#stats-by-time-buckets). For example, the following query
returns per-hour number of logs with the `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word) for the last day:

```logsql
_time:1d error | stats by (_time:1h) count() rows | sort by (_time)
```

This query uses [`sort` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#sort-pipe) in order to sort per-hour stats
by [`_time`](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field).

## How to calculate the number of logs per IPv4 subnetwork?

Use [`stats` by IPv4 bucket](https://docs.victoriametrics.com/victorialogs/logsql/#stats-by-ipv4-buckets). For example, the following
query returns top 10 `/24` subnetworks with the biggest number of logs for the last 5 minutes:

```logsql
_time:5m | stats by (ip:/24) count() rows | last 10 by (rows)
```

This query uses [`first` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#first-pipe) in order to get up to 10 per-subnetwork stats
with the biggest number of rows.

The query assumes the original logs have `ip` [field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model) with the IPv4 address.
If the IPv4 address is located inside [log message](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field) or any other text field,
then it can be extracted with the [`extract`](https://docs.victoriametrics.com/victorialogs/logsql/#extract-pipe)
or [`extract_regexp`](https://docs.victoriametrics.com/victorialogs/logsql/#extract_regexp-pipe) pipes. For example, the following query
extracts IPv4 address from [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field) and then returns top 10
`/16` subnetworks with the biggest number of logs for the last 5 minutes:

```logsql
_time:5m | extract_regexp "(?P<ip>([0-9]+[.]){3}[0-9]+)" | stats by (ip:/16) count() rows | first 10 by (rows desc)
```

## How to calculate the number of logs per every value of the given field?

Use [`stats` by field](https://docs.victoriametrics.com/victorialogs/logsql/#stats-by-fields). For example, the following query
calculates the number of logs per `level` [field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#data-model) for logs over the last 5 minutes:

```logsql
_time:5m | stats by (level) count() rows
```

An alternative is to use [`field_values` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#field_values-pipe):

```logsql
_time:5m | field_values level
```

## How to get unique values for the given field?

Use [`uniq` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#uniq-pipe). For example, the following query returns unique values for the `ip` field
over logs for the last 5 minutes:

```logsql
_time:5m | uniq by (ip)
```

## How to get unique sets of values for the given fields?

Use [`uniq` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#uniq-pipe). For example, the following query returns unique sets for (`host`, `path`) fields
over logs for the last 5 minutes:

```logsql
_time:5m | uniq by (host, path)
```

## How to return last N logs for the given query?

Use [`first` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#first-pipe). For example, the following query returns the last 10 logs with the `error`
[word](https://docs.victoriametrics.com/victorialogs/logsql/#word) in the [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field)
over the logs for the last 5 minutes:

```logsql
_time:5m error | first 10 by (_time desc)
```

It sorts the matching logs by [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field) and then selects
the last 10 logs with the highest values for the `_time` field.

If the query is sent to [`/select/logsql/query` HTTP API](https://docs.victoriametrics.com/victorialogs/querying/#querying-logs), then `limit=N` query arg
can be passed to it in order to return up to `N` latest log entries. For example, the following command returns up to 10 latest log entries with the `error` word:

```sh
curl http://localhost:9428/select/logsql/query -d 'query=error' -d 'limit=10'
```

See also:

- [How to select recently ingested logs?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-select-recently-ingested-logs)
- [How to return last N logs for the given query?](https://docs.victoriametrics.com/victorialogs/logsql-examples/#how-to-return-last-n-logs-for-the-given-query)

## How to calculate the share of error logs to the total number of logs?

Use the following query:

```logsql
_time:5m | stats count() logs, count() if (error) errors | math errors / logs
```

This query uses the following [LogsQL](https://docs.victoriametrics.com/victorialogs/logsql/) features:

- [`_time` filter](https://docs.victoriametrics.com/victorialogs/logsql/#time-filter) for selecting logs on the given time range (last 5 minutes in the query above).
- [`stats` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#stats-pipe) with [additional filtering](https://docs.victoriametrics.com/victorialogs/logsql/#stats-with-additional-filters)
  for calculating the total number of logs and the number of logs with the `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word) on the selected time range.
- [`math` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#math-pipe) for calculating the share of logs with `error` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word)
  comparing to the total number of logs.

## How to select logs for working hours and weekdays?

Use [`day_range`](https://docs.victoriametrics.com/victorialogs/logsql/#day-range-filter) and [`week_range`](https://docs.victoriametrics.com/victorialogs/logsql/#week-range-filter) filters.
For example, the following query selects logs from Monday to Friday in working hours `[08:00 - 18:00]` over the last 4 weeks:

```logsql
_time:4w _time:week_range[Mon, Fri] _time:day_range[08:00, 18:00)
```

It uses implicit [`AND` logical filter](https://docs.victoriametrics.com/victorialogs/logsql/#logical-filter) for joining multiple filters
on [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field).

## How to find logs with the given phrase containing whitespace?

Use [`phrase filter`](https://docs.victoriametrics.com/victorialogs/logsql/#phrase-filter). For example, the following [LogsQL query](https://docs.victoriametrics.com/victorialogs/logsql/)
returns logs with the `cannot open file` phrase over the last 5 minutes:

```logsql
_time:5m "cannot open file"
```

## How to select all the logs for a particular stacktrace or panic?

Use [`stream_context` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#stream_context-pipe) for selecting surrounding logs for the given log.
For example, the following query selects up to 10 logs in front of every log message containing the `stacktrace` [word](https://docs.victoriametrics.com/victorialogs/logsql/#word),
plus up to 100 logs after the given log message:

```logsql
_time:5m stacktrace | stream_context before 10 after 100
```

## How to get the duration since the last seen log entry matching the given filter?

Use the following query:

```logsql
_time:1d ERROR
  | stats max(_time) as max_time
  | math round((now() - max_time) / 1s) as duration_seconds
```

It uses [`max()` stats function](https://docs.victoriametrics.com/victorialogs/logsql/#max-stats) for obtaining the maximum value
for the [`_time` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#time-field) across all the logs for the last day,
which contain the `ERROR` word in the [`_msg` field](https://docs.victoriametrics.com/victorialogs/keyconcepts/#message-field).
Then it uses `now()` function at [`math` pipe](https://docs.victoriametrics.com/victorialogs/logsql/#math-pipe) for calculating
the duration since the last seen log entry with the `ERROR` word.

## How to filter authentication errors by status code?

Authentication and validation errors are usually reported with `4xx` status codes such as `400` or `422`.

The following query returns authentication‑related errors over the last 15 minutes using a time filter and range comparison on `http.status_code`.

```logsql
_time:15m
http.status_code:>=400
http.status_code:<500
| sort by (_time)
```

You may add `path` to filter by URL. Adjust the path string for your logged data:

```logsql
_time:15m
path:="/api/auth"*
http.status_code:>=400
http.status_code:<500
| sort by (_time)
```

If you want to focus only on `400` and `422` errors and see how many of each occurred, use `stats` with grouping by status code.

```logsql
_time:15m
http.status_code:in(400, 422)
| stats by (http.status_code) count() errors
| sort by (errors desc)
```

## How to investigate POST or PATCH requests for a given user?

To inspect all `POST` and `PATCH` requests made by a specific user over the last hour, combine filters on `user.id` and `http.method` and then sort by time.

```logsql
_time:1h
user.id:="12345"
http.method:in("POST","PATCH")
| sort by (_time)
```

If user information and HTTP method are stored inside a JSON payload (for example, in `_msg`), use `unpack_json` to extract structured fields at query time.

```logsql
_time:1h
path:="/api/orders"*
| unpack_json
| filter user.id:="12345" http.method:="POST"
| sort by (_time)
```

## How to track errors by user ID or `X-User-Id` header?

The following query finds the top users with the most `5xx` errors over the last 24 hours, assuming a `user.id` field is present in the error logs.

```logsql
_time:24h
http.status_code:>=500
| stats by (user.id) count() errors
| sort by (errors desc)
| limit 20
```

Sometimes user identity is stored in a header such as `X-User-Id`, in those cases, the same query becomes:

```logsql
_time:24h
http.status_code:>=500
| stats by ("X-User-Id") count() errors
| sort by (errors desc)
| limit 20
```

When the user ID lives inside a JSON payload, you can extract it first and then aggregate.

```logsql
_time:24h
http.status_code:>=400
| unpack_json
| stats by (json.user.id) count() errors
| sort by (errors desc)
| limit 20
```

## How to calculate error rate per second by status code

The `rate()` combined with the `stats` function can be used to return the average per‑second rate of matching logs.

```logsql
http.status_code:>=500
| stats by (http.status_code) rate() errors_per_second
| sort by (errors_per_second desc)
```

## Error rate by client version

Assuming `client.version` is a field,  you can see which versions generate the most `5xx` errors.

```logsql
http.status_code:>=500
| stats by (client.version) rate() errors_per_second, count() errors
| sort by (errors_per_second desc)
```

## Latency percentiles (quantiles) per endpoint

The `quantile(phi, field)` stats function returns an estimated percentile of a numeric field across matching logs (for example, request duration). This query produces per‑endpoint latency percentiles that can be visualized as tables or charts in your dashboard.

```logsql
path:="/api/"*
http.status_code:<500
| stats by (path)
    quantile(0.5, http.request_duration_ms)  p50_ms,
    quantile(0.9, http.request_duration_ms)  p90_ms,
    quantile(0.99, http.request_duration_ms) p99_ms
| sort by (p99_ms desc)
```

## How to search for users on a specific app, major, and minor version

LogsQL supports prefix, substring, and regex filters on arbitrary fields, including `client.version` and `user.name`.

If versions are stored as strings like `2.3.4` in `client.version`, you can match a whole minor version via exact prefix filter or regex.

```logsql
client.version:="2.3."*
| uniq by (user.id)
```

Or with a regexp filter:

```logsql
client.version:~"^2\\.3\\."
| uniq by (user.id)
```

To see how many users per version are active over the last 7 days:

```logsql
_time:7d
| stats by (client.version) count_uniq(user.id) users
| sort by (users desc)
```

## How to fuzzy search usernames

To find usernames containing a substring (for example, all usernames containing `john`), you can use a substring filter on the `user.name` field.

```logsql
user.name:*john*
| sort by (_time) desc
```

For case‑insensitive matching, use a regex with the `(?i)` flag.

```logsql
user.name:~"(?i)john"
| uniq by (user.name)
```

## How to filter out paths or messages you are not interested in?

To exclude log lines containing a specific substring (for example, health checks), prepend the word or phrase filter with `-`.

```logsql
"error"
-"healthcheck"
| sort by (_time) desc
```

This returns logs that contain `error` but do not contain `healthcheck` anywhere in the line.

You can apply the same pattern to authentication errors, for example, excluding `/healthz` and `/metrics` endpoints while looking at `4xx` codes.

```logsql
http.status_code:>=400
http.status_code:<500
-"GET /healthz"
-"/metrics"
| sort by (_time)
```

## How to exclude specific status codes or methods?

Field filters can also be negated with `-` in front of them. For example, "all errors except 404":

```logsql
http.status_code:>=400
-http.status_code:=404
| stats by (http.status_code) count() errors
| sort by (errors desc)
```

Or "all requests from this user that are **not** `GET` requests":

```logsql
user.id:="12345"
-http.method:="GET"
| sort by (_time)
```

## How to exclude environments, client versions, or users?

When filtering by labels/fields such as environment or client version, you can use `-field:=value` or `-field:in(...)` to exclude sets of values.

Exclude logs from non‑production environments while analyzing error rates:

```logsql
http.status_code:>=500
-env:in("dev","staging","test")
| stats by (_time:5m) count() errors
| sort by (_time)
```

Exclude a specific problematic client version from your "error rate by client version":

```logsql
http.status_code:>=500
-client.version:="1.0.0-beta"
| stats by (client.version) rate() errors_per_second, count() errors
| sort by (errors_per_second desc)
```

You can also flip your "top 20 users by errors" example to ignore an internal test user ID:

```logsql
http.status_code:>=500
-user.id:="internal-test-user"
| stats by (user.id) count() errors
| sort by (errors desc)
| limit 20
```

## How to use negative regex filters?

For message‑level regexes, LogsQL supports negative matching with `NOT ~"regexp"`. Exclude logs whose messages match a noisy pattern:

```logsql
"error"
NOT ~"^ignored_error_code_[0-9]+"
| sort by (_time) desc
```

For label fields, use `-field:~"regexp"` to exclude values matching a regex.

```logsql
-client.version:~"^0\\."
| stats by (client.version) uniq(user.id) users
| sort by (users desc)
```

## How to negate whole logical conditions with NOT?

In addition to the shorthand `-filter`, LogsQL supports logical filters `AND`, `OR`, and `NOT` to combine or negate groups of filters.

For example, "all warnings that are not from nginx or envoy":

```logsql
level:="warning"
NOT (app:="nginx" OR app:="envoy")
| sort by (_time) desc
```

Or, in a signup investigation, "failed signups that are not from bots":

```logsql
  path:="/api/signup"
  http.method:="POST"
  | unpack_json
  | filter json.status:in("error","failed")
            AND NOT json.user.agent:~"bot|crawler"
  | stats by (_time:5m) count() errors
  | sort by (_time)
```


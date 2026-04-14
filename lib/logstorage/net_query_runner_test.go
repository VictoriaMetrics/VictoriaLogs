package logstorage

import (
	"strings"
	"testing"
)

func TestSplitQueryToRemoteAndLocal(t *testing.T) {
	f := func(qStr, remoteQueryExpected, localPipesExpected string) {
		t.Helper()

		q, err := ParseQuery(qStr)
		if err != nil {
			t.Fatalf("cannot parse query: %s", err)
		}

		qStrBefore := q.String()
		qRemote, pipesLocal := splitQueryToRemoteAndLocal(q)
		qStrAfter := q.String()

		if qStrBefore != qStrAfter {
			t.Fatalf("the query unexpectedly changed in splitQueryToRemoteAndLocal()\noriginal query\n%s\nresulting query\n%s", qStrBefore, qStrAfter)
		}

		remoteQuery := qRemote.String()
		if remoteQuery != remoteQueryExpected {
			t.Fatalf("unexpected remote query\ngot\n%s\nwant\n%s", remoteQuery, remoteQueryExpected)
		}

		a := make([]string, len(pipesLocal))
		for i, p := range pipesLocal {
			a[i] = p.String()
		}
		localPipes := strings.Join(a, " | ")
		if localPipes != localPipesExpected {
			t.Fatalf("unexpected local pipes\ngot\n%s\nwant\n%s", localPipes, localPipesExpected)
		}
	}

	f("*", "*", "")
	f(`* | format "foo<bar>" | count() x`, `* | format "foo<bar>" | stats_remote count(*) as x`, `stats_local import_state(x) as x`)
	f(`foo | sort by (_time desc) | limit 30 | keep a, _time`, `foo | sort by (_time desc) limit 30 | fields _time, a`, `sort by (_time desc) limit 30 | fields a, _time`)
	f(`foo | sort by (_time desc) | limit 0 | keep a, _time`, `foo | limit 0 | fields _time, a`, `limit 0 | fields a, _time`)
	f(`foo | sort by (_time desc) | offset 0 | limit 30 | keep a, _time`, `foo | sort by (_time desc) limit 30 | fields _time, a`, `sort by (_time desc) limit 30 | fields a, _time`)
	f(`foo | sort by (_time desc) | offset 10 | limit 30 | keep a, _time`, `foo | sort by (_time desc) limit 40 | fields _time, a`, `sort by (_time desc) offset 10 limit 30 | fields a, _time`)

	f(`foo | blocks_count`, `foo | blocks_count`, `stats sum("blocks_count") as "blocks_count"`)
	f(`foo | blocks_count as x`, `foo | blocks_count as x`, `stats sum(x) as x`)
	f(`foo | block_stats`, `foo | block_stats`, ``)
	f(`foo | collapse_nums`, `foo | collapse_nums`, ``)
	f(`foo | copy a as b`, `foo | copy a as b`, ``)
	f(`foo | decolorize`, `foo | decolorize`, ``)
	f(`foo | delete x`, `foo | delete x`, ``)
	f(`foo | drop_empty_fields`, `foo | drop_empty_fields`, ``)
	f(`foo | extract "foo<bar>baz"`, `foo | extract "foo<bar>baz"`, ``)
	f(`foo | extract_regexp "foo(?P<ip>[^;]+)"`, `foo | extract_regexp "foo(?P<ip>[^;]+)"`, ``)
	f(`foo | facets`, `foo | facets 18446744073709551615`, `stats by (field_name, field_value) sum(hits) as hits | total_stats by (field_name) count(*) as field_values_count | filter field_values_count:<=1000 | delete field_values_count | sort by (hits desc) partition by (field_name) limit 10 | sort by (field_name, hits desc, field_value) | fields field_name, field_value, hits`)
	f(`foo | field_names`, `foo | field_names`, `stats by (name) sum(hits) as hits`)
	f(`foo | field_names as hits`, `foo | field_names as hitss`, `stats by (hitss) sum(hits) as hits | rename hitss as hits`)
	f(`foo | field_values x`, `foo | field_values x`, `field_values_local x`)
	f(`foo | fields x, y`, `foo | fields x, y`, ``)
	f(`foo | filter a:b`, `foo a:b`, ``)
	f(`foo | first 10 by (x)`, `foo | sort by (x) limit 10`, `sort by (x) limit 10`)
	f(`foo | format "x<y>"`, `foo | format "x<y>"`, ``)
	f(`foo | generate_sequence 10`, `foo | delete *`, `generate_sequence 10`)
	f(`foo | join by (x) (bar)`, `foo`, `join by (x) (bar)`)
	f(`foo | json_array_len (x) y`, `foo | json_array_len(x) as y`, ``)
	f(`foo | hash(x) as y`, `foo | hash(x) as y`, ``)
	f(`foo | last 10 by (x)`, `foo | sort by (x) desc limit 10`, `sort by (x) desc limit 10`)
	f(`foo | len(x) as y`, `foo | len(x) as y`, ``)
	f(`foo | limit 10`, `foo | limit 10`, `limit 10`)
	f(`foo | math x+y as z`, `foo | math (x + y) as z`, ``)
	f(`foo | offset 10`, `foo`, `offset 10`)
	f(`foo | pack_json`, `foo | pack_json`, ``)
	f(`foo | pack_logfmt`, `foo | pack_logfmt`, ``)
	f(`foo | query_stats`, `foo | query_stats`, `query_stats_local`)
	f(`foo | rename x as y`, `foo | rename x as y`, ``)
	f(`foo | replace ("x", "y")`, `foo | replace (x, y)`, ``)
	f(`foo | replace_regexp ("x", "y")`, `foo | replace_regexp (x, y)`, ``)
	f(`foo | running_stats by (x) sum(y) as z`, `foo | delete z`, `running_stats by (x) sum(y) as z`)
	f(`foo | sample 10`, `foo | sample 10`, ``)
	f(`foo | split ","`, `foo | split ","`, ``)
	f(`foo | stats by (x) count() as y`, `foo | stats_remote by (x) count(*) as y`, `stats_local by (x) import_state(y) as y`)
	f(`foo | stats by (x,a) count() as y, sum(q) as b`, `foo | stats_remote by (x, a) count(*) as y, sum(q) as b`, `stats_local by (x, a) import_state(y) as y, import_state(b) as b`)
	f(`foo | stream_context before 10 after 3`, `foo`, `stream_context before 10 after 3`)
	f(`foo | time_add 1h`, `foo | time_add 1h`, ``)
	f(`foo | top 10 by (x)`, `foo | stats by (x) count(*) as hits`, `stats by (x) sum(hits) as hits | first 10 by (hits desc, x) | fields x, hits`)
	f(`foo | top 10 by (x) rank`, `foo | stats by (x) count(*) as hits`, `stats by (x) sum(hits) as hits | first 10 by (hits desc, x) rank | fields x, hits, rank`)
	f(`foo | top 10 by (x) rank as y`, `foo | stats by (x) count(*) as hits`, `stats by (x) sum(hits) as hits | first 10 by (hits desc, x) rank as y | fields x, hits, y`)
	f(`foo | total_stats by (x) sum(y) as z`, `foo | delete z`, `total_stats by (x) sum(y) as z`)
	f(`foo | union (bar)`, `foo`, `union (bar)`)
	f(`foo | uniq by (x)`, `foo | uniq by (x)`, `uniq by (x)`)
	f(`foo | uniq by (x) limit 3`, `foo | uniq by (x) limit 3`, `uniq by (x) limit 3`)
	f(`foo | uniq by (x) hits`, `foo | uniq by (x) with hits`, `uniq_local by (x) limit 0`)
	f(`foo | uniq by (x) hits limit 5`, `foo | uniq by (x) with hits limit 5`, `uniq_local by (x) limit 5`)
	f(`foo | unpack_json`, `foo | unpack_json`, ``)
	f(`foo | unpack_logfmt`, `foo | unpack_logfmt`, ``)
	f(`foo | unpack_syslog`, `foo | unpack_syslog`, ``)
	f(`foo | unpack_words`, `foo | unpack_words`, ``)
	f(`foo | unroll by (x)`, `foo | unroll by (x)`, ``)

	// Special cases with 'offset 0'
	// See https://github.com/VictoriaMetrics/VictoriaLogs/issues/620#issuecomment-3276624504
	f(`foo | offset 0`, `foo`, ``)
	f(`foo | offset 0 | limit 10`, `foo | limit 10`, `limit 10`)

	f(`foo | offset 5 | limit 10`, `foo | limit 15`, `limit 15 | offset 5`)
	f(`foo | limit 35 | offset 10 | offset 20 | limit 7`, `foo | limit 35`, `limit 35 | offset 30`)

	// Make sure the options(..) is correctly propagated to both local and remote parts of the query
	f(`options(concurrency=10) foo | join by (x) (bar:in(a | keep x))`, `options(concurrency=10) foo`, `join by (x) (options(concurrency=10) bar:in(options(concurrency=10) a | fields x))`)
	f(`options(parallel_readers=10) foo | join by (x) (bar:in(options(parallel_readers=1) a | keep x))`, `options(parallel_readers=10) foo`, `join by (x) (options(parallel_readers=10) bar:in(options(parallel_readers=1) a | fields x))`)
	f(`options(ignore_global_time_filter=true) foo | join by (x) (bar:in(a | keep x))`, `options(ignore_global_time_filter=true) foo`, `join by (x) (options(ignore_global_time_filter=true) bar:in(options(ignore_global_time_filter=true) a | fields x))`)
	f(`options(allow_partial_response=true) foo | join by (x) (bar:in(a | keep x))`, `options(allow_partial_response=true) foo`, `join by (x) (options(allow_partial_response=true) bar:in(options(allow_partial_response=true) a | fields x))`)
	f(`options(time_offset=7d) foo | join by (x) (bar:in(a | keep x))`, `options(time_offset=7d) foo`, `join by (x) (options(time_offset=7d) bar:in(options(time_offset=7d) a | fields x))`)
	f(`options(global_filter=(_time:5m)) foo | join by (x) (bar:in(a | keep x))`, `options(global_filter=(_time:5m)) foo`, `join by (x) (options(global_filter=(_time:5m)) bar:in(options(global_filter=(_time:5m)) a | fields x))`)
}

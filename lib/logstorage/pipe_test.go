package logstorage

import (
	"testing"
)

// TestPipeParsers_allPipesAreClassified fails when a pipe is added to pipeParsers without
// deciding whether it belongs in isOneRowPerRowPipe.
//
// A pipe missing from isOneRowPerRowPipe blocks the 'sort ... | limit ...' merge instead of
// breaking anything, so the omission is silent. This test makes it loud.
func TestPipeParsers_allPipesAreClassified(t *testing.T) {
	knownPipes := []string{
		"block_stats", "blocks_count", "coalesce", "collapse_nums", "copy", "cp", "decolorize", "del", "delete",
		"drop", "drop_empty_fields", "eval", "extract", "extract_regexp", "facets", "field_names", "field_values",
		"fields", "filter", "first", "format", "generate_sequence", "hash", "head", "join", "json_array_concat",
		"json_array_len", "keep", "last", "len", "limit", "math", "mv", "offset", "order", "pack_json",
		"pack_logfmt", "query_stats", "rename", "replace", "replace_regexp", "rm", "running_stats", "sample",
		"set_stream_fields", "skip", "sort", "split", "stats", "stats_remote", "stream_context", "time_add", "top",
		"total_stats", "union", "uniq", "unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_words", "unroll",
		"where",
	}

	m := make(map[string]struct{}, len(knownPipes))
	for _, name := range knownPipes {
		m[name] = struct{}{}
	}

	parsers := getPipeParsers()
	for name := range parsers {
		if _, ok := m[name]; !ok {
			t.Fatalf("unknown pipe %q; add it to knownPipes here, and to isOneRowPerRowPipe at lib/logstorage/parser.go "+
				"if it writes a single output row per every input row", name)
		}
	}
	for _, name := range knownPipes {
		if _, ok := parsers[name]; !ok {
			t.Fatalf("pipe %q is missing from pipeParsers; drop it from knownPipes here", name)
		}
	}
}

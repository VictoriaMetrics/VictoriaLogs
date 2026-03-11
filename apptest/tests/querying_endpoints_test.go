package tests

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleFieldNamesResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
		`{"_time":"2025-06-06T14:30:19.088007Z","foo":"bar","x":"z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query string, ignorePipes bool, responseExpected string) {
		t.Helper()

		ignorePipesStr := ""
		if ignorePipes {
			ignorePipesStr = "1"
		}

		response := sut.FieldNames(t, query, apptest.FieldNamesOpts{
			IgnorePipes: ignorePipesStr,
		})
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// 'select all' query
	query := "*"
	responseExpected := `{"values":[{"value":"_msg","hits":2},{"value":"_stream","hits":2},{"value":"_stream_id","hits":2},{"value":"_time","hits":2},{"value":"foo","hits":2},{"value":"x","hits":2}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs matching x:=y
	query = "x:=y"
	responseExpected = `{"values":[{"value":"_msg","hits":1},{"value":"_stream","hits":1},{"value":"_stream_id","hits":1},{"value":"_time","hits":1},{"value":"foo","hits":1},{"value":"x","hits":1}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs with additional pipe
	query = "* | format 'abc' as new_field"
	responseExpected = `{"values":[{"value":"_msg","hits":2},{"value":"_stream","hits":2},{"value":"_stream_id","hits":2},{"value":"_time","hits":2},{"value":"foo","hits":2},{"value":"new_field","hits":2},{"value":"x","hits":2}]}`
	f(query, false, responseExpected)

	responseExpected = `{"values":[{"value":"_msg","hits":2},{"value":"_stream","hits":2},{"value":"_stream_id","hits":2},{"value":"_time","hits":2},{"value":"foo","hits":2},{"value":"x","hits":2}]}`
	f(query, true, responseExpected)
}

func TestVlsingleFieldValuesResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
		`{"_time":"2025-06-06T14:30:19.088007Z","foo":"bar","x":"z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{})
	sut.ForceFlush(t)

	f := func(query, field string, ignorePipes bool, responseExpected string) {
		t.Helper()

		ignorePipesStr := ""
		if ignorePipes {
			ignorePipesStr = "1"
		}

		response := sut.FieldValues(t, query, apptest.FieldValuesOpts{
			Field:       field,
			IgnorePipes: ignorePipesStr,
		})
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// 'select all' query
	query := "*"
	field := "x"
	responseExpected := `{"values":[{"value":"y","hits":1},{"value":"z","hits":1}]}`
	f(query, field, false, responseExpected)
	f(query, field, true, responseExpected)

	// select logs matching x:=y
	query = "x:=y"
	field = "foo"
	responseExpected = `{"values":[{"value":"bar","hits":1}]}`
	f(query, field, false, responseExpected)
	f(query, field, true, responseExpected)

	// select logs with additional pipe
	query = "* | format 'abc' as new_field"
	field = "new_field"
	responseExpected = `{"values":[{"value":"abc","hits":2}]}`
	f(query, field, false, responseExpected)

	responseExpected = `{"values":[{"value":"","hits":2}]}`
	f(query, field, true, responseExpected)
}

func TestVlsingleStreamFieldNamesResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
		`{"_time":"2025-06-06T14:30:19.088007Z","foo":"bar","x":"z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{
		StreamFields: "foo,x",
	})
	sut.ForceFlush(t)

	f := func(query string, ignorePipes bool, responseExpected string) {
		t.Helper()

		ignorePipesStr := ""
		if ignorePipes {
			ignorePipesStr = "1"
		}

		response := sut.StreamFieldNames(t, query, apptest.StreamFieldNamesOpts{
			IgnorePipes: ignorePipesStr,
		})
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// 'select all' query
	query := "*"
	responseExpected := `{"values":[{"value":"foo","hits":2},{"value":"x","hits":2}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs matching x:=y
	query = "x:=y"
	responseExpected = `{"values":[{"value":"foo","hits":1},{"value":"x","hits":1}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs with additional pipe
	query = "* | format 'abc' as new_field | set_stream_fields new_field, x"
	responseExpected = `{"values":[{"value":"new_field","hits":2},{"value":"x","hits":2}]}`
	f(query, false, responseExpected)

	responseExpected = `{"values":[{"value":"foo","hits":2},{"value":"x","hits":2}]}`
	f(query, true, responseExpected)
}

func TestVlsingleStreamFieldValuesResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
		`{"_time":"2025-06-06T14:30:19.088007Z","foo":"bar","x":"z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{
		StreamFields: "foo,x",
	})
	sut.ForceFlush(t)

	f := func(query, field string, ignorePipes bool, responseExpected string) {
		t.Helper()

		ignorePipesStr := ""
		if ignorePipes {
			ignorePipesStr = "1"
		}

		response := sut.StreamFieldValues(t, query, apptest.StreamFieldValuesOpts{
			Field:       field,
			IgnorePipes: ignorePipesStr,
		})
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// 'select all' query
	query := "*"
	field := "x"
	responseExpected := `{"values":[{"value":"y","hits":1},{"value":"z","hits":1}]}`
	f(query, field, false, responseExpected)
	f(query, field, true, responseExpected)

	// select logs matching x:=y
	query = "x:=y"
	field = "foo"
	responseExpected = `{"values":[{"value":"bar","hits":1}]}`
	f(query, field, false, responseExpected)
	f(query, field, true, responseExpected)

	// select logs with additional pipe
	query = "* | format 'abc' as new_field | set_stream_fields new_field, x"
	field = "new_field"
	responseExpected = `{"values":[{"value":"abc","hits":2}]}`
	f(query, field, false, responseExpected)

	responseExpected = `{"values":[]}`
	f(query, field, true, responseExpected)
}

func TestVlsingleStreamsResponse(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	sut := tc.MustStartDefaultVlsingle()

	ingestRecords := []string{
		`{"_time":"2025-06-05T14:30:19.088007Z","foo":"bar","x":"y"}`,
		`{"_time":"2025-06-06T14:30:19.088007Z","foo":"bar","x":"z"}`,
	}
	sut.JSONLineWrite(t, ingestRecords, apptest.IngestOpts{
		StreamFields: "foo,x",
	})
	sut.ForceFlush(t)

	f := func(query string, ignorePipes bool, responseExpected string) {
		t.Helper()

		ignorePipesStr := ""
		if ignorePipes {
			ignorePipesStr = "1"
		}

		response := sut.Streams(t, query, apptest.StreamsOpts{
			IgnorePipes: ignorePipesStr,
		})
		if response != responseExpected {
			t.Fatalf("unexpected response\ngot\n%s\nwant\n%s", response, responseExpected)
		}
	}

	// 'select all' query
	query := "*"
	responseExpected := `{"values":[{"value":"{foo=\"bar\",x=\"y\"}","hits":1},{"value":"{foo=\"bar\",x=\"z\"}","hits":1}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs matching x:=y
	query = "x:=y"
	responseExpected = `{"values":[{"value":"{foo=\"bar\",x=\"y\"}","hits":1}]}`
	f(query, false, responseExpected)
	f(query, true, responseExpected)

	// select logs with additional pipe
	query = "* | format 'abc' as new_field | set_stream_fields new_field, x"
	responseExpected = `{"values":[{"value":"{new_field=\"abc\",x=\"y\"}","hits":1},{"value":"{new_field=\"abc\",x=\"z\"}","hits":1}]}`
	f(query, false, responseExpected)

	responseExpected = `{"values":[{"value":"{foo=\"bar\",x=\"y\"}","hits":1},{"value":"{foo=\"bar\",x=\"z\"}","hits":1}]}`
	f(query, true, responseExpected)
}

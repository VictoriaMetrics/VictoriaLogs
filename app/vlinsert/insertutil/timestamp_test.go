package insertutil

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestExtractTimestampFromFields_Success(t *testing.T) {
	f := func(timeField string, fields []logstorage.Field, nsecsExpected int64) {
		t.Helper()

		nsecs, err := ExtractTimestampFromFields([]string{timeField}, fields)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if nsecs != nsecsExpected {
			t.Fatalf("unexpected nsecs; got %d; want %d", nsecs, nsecsExpected)
		}

		for _, f := range fields {
			if f.Name == timeField {
				if f.Value != "" {
					t.Fatalf("unexpected value for field %s; got %q; want %q", timeField, f.Value, "")
				}
			}
		}
	}

	// UTC time
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "2024-06-18T23:37:20Z"},
	}, 1718753840000000000)

	// Time with timezone
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "2024-06-18T23:37:20+08:00"},
	}, 1718725040000000000)

	// SQL datetime format
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "2024-06-18 23:37:20.123-05:30"},
	}, 1718773640123000000)

	// Time with nanosecond precision
	f("time", []logstorage.Field{
		{Name: "time", Value: "2024-06-18T23:37:20.123456789-05:30"},
		{Name: "foo", Value: "bar"},
	}, 1718773640123456789)

	// Unix timestamp in nanoseconds
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "1718773640123456789"},
	}, 1718773640123456789)

	// Unix timestamp in microseconds
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "1718773640123456"},
	}, 1718773640123456000)

	// Unix timestamp in milliseconds
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "1718773640123"},
	}, 1718773640123000000)

	// Unix timestamp in seconds
	f("time", []logstorage.Field{
		{Name: "foo", Value: "bar"},
		{Name: "time", Value: "1718773640"},
	}, 1718773640000000000)
}

func TestExtractTimestampFromFields_Now(t *testing.T) {
	f := func(timeField string, fields []logstorage.Field) {
		t.Helper()

		nsecs, err := ExtractTimestampFromFields([]string{timeField}, fields)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if nsecs < 1 {
			t.Fatalf("expected generated timestamp, got error: %s", err)
		}
	}

	// RFC5424 allows `-` for nil timestamp (log ingestion time)
	f("time", []logstorage.Field{
		{Name: "time", Value: "-"},
	})

	f("time", []logstorage.Field{
		{Name: "time", Value: ""},
	})

	f("time", []logstorage.Field{
		{Name: "time", Value: "0"},
	})
}

func TestExtractTimestampFromFields_Error(t *testing.T) {
	f := func(s string) {
		t.Helper()

		fields := []logstorage.Field{
			{Name: "time", Value: s},
		}
		nsecs, err := ExtractTimestampFromFields([]string{"time"}, fields)
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if nsecs != 0 {
			t.Fatalf("unexpected nsecs; got %d; want %d", nsecs, 0)
		}
	}

	// invalid time
	f("foobar")

	// incomplete time
	f("2024-06-18")
	f("2024-06-18T23:37")
}

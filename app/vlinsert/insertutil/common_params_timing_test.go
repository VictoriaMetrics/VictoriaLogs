package insertutil

import (
	"net/http"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func BenchmarkAddRow(b *testing.B) {
	r, err := http.NewRequest(http.MethodPost, "https://foo.bar/baz?_stream_fields=kubernetes.pod_name,kubernetes.pod_labels.app", nil)
	if err != nil {
		b.Fatal(err)
	}
	cp, err := GetCommonParams(r)
	if err != nil {
		b.Fatal(err)
	}

	SetLogRowsStorage(benchmarkStorage{})

	ts := int64(fasttime.UnixTimestamp()) * 1e9
	rows := [][]logstorage.Field{
		{
			{Name: "kubernetes.pod_name", Value: "victoria-logs-single-0"},
			{Name: "kubernetes.pod_labels.app", Value: "VictoriaLogs"},
			{Name: "kubernetes.container_name", Value: "oauth2-proxy"},
			{Name: "_msg", Value: `GET - "/ready" HTTP/1.1 "kube-probe/1.34" 200 2 0.000`},
		},
		{
			{Name: "kubernetes.pod_name", Value: "victoria-logs-single-0"},
			{Name: "kubernetes.pod_labels.app", Value: "VictoriaLogs"},
			{Name: "kubernetes.container_name", Value: "oauth2-proxy"},
			{Name: "_msg", Value: `GET - "/ready" HTTP/1.1 "GoogleHC/1.0" 200 2 0.000`},
		},
	}

	var n int
	for _, row := range rows {
		n += logstorage.EstimatedJSONRowLen(row)
	}

	b.SetBytes(int64(n))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		lmp := cp.NewLogMessageProcessor("test", false)
		defer lmp.MustClose()

		for pb.Next() {
			for _, row := range rows {
				lmp.AddRow(ts, row, -1)
			}
		}
	})
}

type benchmarkStorage struct{}

func (benchmarkStorage) MustAddRows(*logstorage.LogRows) {
}

func (benchmarkStorage) CanWriteData() error {
	return nil
}

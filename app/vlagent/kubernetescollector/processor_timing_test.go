package kubernetescollector

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func BenchmarkProcessorFullLines(b *testing.B) {
	data := []string{
		"2025-10-16T15:37:36.330062387Z stderr F foo",
		"2025-10-16T15:37:36.330062387Z stderr F bar",
		"2025-10-16T15:37:36.330062387Z stderr F buz",
	}
	benchmarkProcessor(b, data)
}

func BenchmarkProcessorPartialLines(b *testing.B) {
	in := []string{
		"2025-10-16T15:37:36.330062387Z stderr P foo",
		"2025-10-16T15:37:36.330062387Z stderr P bar",
		"2025-10-16T15:37:36.330062387Z stderr F buz",
	}
	benchmarkProcessor(b, in)
}

func BenchmarkProcessorKlog(b *testing.B) {
	in := []string{
		`2025-12-15T10:34:25.637326000Z stderr F I1215 10:34:25.637326       1 serving.go:374] Generated self-signed cert (/tmp/apiserver.crt, /tmp/apiserver.key)`,
		`2025-12-15T10:34:25.872911000Z stderr F I1215 10:34:25.872911       1 handler.go:275] Adding GroupVersion metrics.k8s.io v1beta1 to ResourceManager`,
		`2025-12-15T10:34:25.977313000Z stderr F I1215 10:34:25.977313       1 requestheader_controller.go:169] Starting RequestHeaderAuthRequestController`,
		`2025-12-15T10:34:25.977317000Z stderr F I1215 10:34:25.977317       1 configmap_cafile_content.go:202] "Starting controller" name="client-ca::kube-system::extension-apiserver-authentication::client-ca-file"`,
		`2025-12-15T10:34:25.977332000Z stderr F I1215 10:34:25.977332       1 shared_informer.go:311] Waiting for caches to sync for RequestHeaderAuthRequestController`,
		`2025-12-15T10:34:25.977336000Z stderr F I1215 10:34:25.977336       1 shared_informer.go:311] Waiting for caches to sync for client-ca::kube-system::extension-apiserver-authentication::requestheader-client-ca-file`,
		`2025-12-15T10:34:25.977526000Z stderr F I1215 10:34:25.977526       1 dynamic_serving_content.go:132] "Starting controller" name="serving-cert::/tmp/apiserver.crt::/tmp/apiserver.key"`,
		`2025-12-15T10:34:25.977591000Z stderr F I1215 10:34:25.977591       1 secure_serving.go:213] Serving securely on [::]:10250`,
		`2025-12-15T10:34:25.977605000Z stderr F I1215 10:34:25.977605       1 tlsconfig.go:240] "Starting DynamicServingCertificateController"`,
		`2025-12-15T10:34:26.077533000Z stderr F I1215 10:34:26.077533       1 shared_informer.go:318] Caches are synced for RequestHeaderAuthRequestController`,
		`2025-12-15T10:34:26.948143000Z stderr F I1215 10:34:26.948143       1 server.go:191] "Failed probe" probe="metric-storage-ready" err="no metrics to serve"`,
	}
	benchmarkProcessor(b, in)
}

func benchmarkProcessor(b *testing.B, logLines []string) {
	totalSize := 0

	var rawLines [][]byte
	for _, s := range logLines {
		totalSize += len(s)
		rawLines = append(rawLines, []byte(s))
	}
	b.SetBytes(int64(totalSize))
	b.ReportAllocs()

	commonFields := []logstorage.Field{{Name: "name", Value: "benchmarkProcessor"}}
	storage := &benchmarkStorage{}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			proc := newLogFileProcessor(storage, commonFields)
			for _, line := range rawLines {
				proc.tryAddLine(line)
			}
			proc.mustClose()
		}
	})
}

type benchmarkStorage struct{}

func (s *benchmarkStorage) MustAddRows(*logstorage.LogRows) {
}

func (s *benchmarkStorage) CanWriteData() error {
	return nil
}

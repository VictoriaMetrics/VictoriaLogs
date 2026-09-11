package kubernetescollector

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestIsCSIIncluded(t *testing.T) {
	originalHdd := *csiRbdHdd
	originalSsd := *csiRbdSsd
	originalNvme := *csiRbdNvme
	*csiRbdHdd = false
	*csiRbdSsd = false
	*csiRbdNvme = false
	t.Cleanup(func() {
		*csiRbdHdd = originalHdd
		*csiRbdSsd = originalSsd
		*csiRbdNvme = originalNvme
	})

	fields := []logstorage.Field{
		{Name: "kubernetes.pod_namespace", Value: "rbd-hdd"},
		{Name: "kubernetes.pod_labels.release", Value: "rbd-hdd"},
		{Name: "kubernetes.pod_labels.component", Value: "nodeplugin"},
	}
	if isCSIIncluded(fields) {
		t.Fatal("CSI logs must be excluded by default")
	}

	*csiRbdHdd = true
	if !isCSIIncluded(fields) {
		t.Fatal("CSI logs must be included when the matching switch is enabled")
	}

	fields[1].Value = "rbd-ssd"
	if isCSIIncluded(fields) {
		t.Fatal("CSI logs with a mismatched release label must be excluded")
	}
}

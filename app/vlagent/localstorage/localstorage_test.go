package localstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestRouteForFields(t *testing.T) {
	original := *routeByKubernetes
	*routeByKubernetes = true
	t.Cleanup(func() {
		*routeByKubernetes = original
	})

	tests := []struct {
		name   string
		fields []logstorage.Field
		want   string
	}{
		{
			name: "virt-launcher",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "default"},
				{Name: "kubernetes.pod_labels.kubevirt.io", Value: "virt-launcher"},
			},
			want: "kubevirt/virt-launcher.log",
		},
		{
			name: "virt-api",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "kubevirt"},
				{Name: "kubernetes.pod_labels.kubevirt.io", Value: "virt-api"},
				{Name: "kubernetes.pod_labels.app.kubernetes.io/component", Value: "kubevirt"},
				{Name: "kubernetes.pod_labels.app.kubernetes.io/managed-by", Value: "virt-operator"},
			},
			want: "kubevirt/virt-api.log",
		},
		{
			name: "cdi-operator",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "cdi"},
				{Name: "kubernetes.pod_labels.name", Value: "cdi-operator"},
				{Name: "kubernetes.pod_labels.operator.cdi.kubevirt.io", Value: ""},
			},
			want: "cdi/cdi-operator.log",
		},
		{
			name: "csi-rbd-hdd-nodeplugin",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "rbd-hdd"},
				{Name: "kubernetes.pod_labels.release", Value: "rbd-hdd"},
				{Name: "kubernetes.pod_labels.component", Value: "nodeplugin"},
			},
			want: "csi/csi-rbd-hdd-nodeplugin.log",
		},
		{
			name: "csi-rbd-nvme-provisioner",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "rbd-nvme"},
				{Name: "kubernetes.pod_labels.release", Value: "rbd-nvme"},
				{Name: "kubernetes.pod_labels.component", Value: "provisioner"},
			},
			want: "csi/csi-rbd-nvme-provisioner.log",
		},
		{
			name: "unknown",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_namespace", Value: "default"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RouteForFields(tt.fields); got != tt.want {
				t.Fatalf("RouteForFields() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConfigForFile(t *testing.T) {
	virtLauncher := configForFile("kubevirt/virt-launcher.log")
	if virtLauncher != virtLauncherFileConfig() {
		t.Fatalf("configForFile(virt-launcher) = %+v, want %+v", virtLauncher, virtLauncherFileConfig())
	}

	common := configForFile("cdi/cdi-apiserver.log")
	if common != defaultFileConfig() {
		t.Fatalf("configForFile(common) = %+v, want %+v", common, defaultFileConfig())
	}
}

func TestNextRotatedPathSkipsCompressedBackup(t *testing.T) {
	dir := t.TempDir()
	fw := &fileWriter{
		dir:             dir,
		filePrefix:      "virt-launcher",
		rotatedExt:      ".log",
		useLocalTime:    true,
		compressRotated: true,
	}

	base := filepath.Join(dir, "virt-launcher."+time.Now().Format("20060102-150405.000")+".log.gz")
	if err := os.WriteFile(base, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got := fw.nextRotatedPathLocked()
	if got == base {
		t.Fatalf("nextRotatedPathLocked() reused compressed backup %q", base)
	}
}

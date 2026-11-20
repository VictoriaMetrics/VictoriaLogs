package localstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestRouteForFields(t *testing.T) {
	original := parsedKubernetesRoutes
	parsedKubernetesRoutes = []kubernetesRoute{
		{
			File: "teams/payments.log",
			Labels: map[string]string{
				"team": "payments",
			},
		},
		{
			File: "teams/payments-backend.log",
			Labels: map[string]string{
				"team": "payments",
				"tier": "backend",
			},
		},
		{
			File: "apps/api.log",
			Labels: map[string]string{
				"app.kubernetes.io/name": "api",
				"environment":            "production",
			},
		},
		{
			File: "ties/first.log",
			Labels: map[string]string{
				"environment": "production",
			},
		},
		{
			File: "ties/second.log",
			Labels: map[string]string{
				"tier": "backend",
			},
		},
	}
	t.Cleanup(func() {
		parsedKubernetesRoutes = original
	})

	tests := []struct {
		name   string
		fields []logstorage.Field
		want   string
	}{
		{
			name: "all labels match",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.app.kubernetes.io/name", Value: "api"},
				{Name: "kubernetes.pod_labels.environment", Value: "production"},
			},
			want: "apps/api.log",
		},
		{
			name: "most specific route wins",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.team", Value: "payments"},
				{Name: "kubernetes.pod_labels.tier", Value: "backend"},
			},
			want: "teams/payments-backend.log",
		},
		{
			name: "single label route",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.team", Value: "payments"},
			},
			want: "teams/payments.log",
		},
		{
			name: "equal specificity keeps first route",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.environment", Value: "production"},
				{Name: "kubernetes.pod_labels.tier", Value: "backend"},
			},
			want: "ties/first.log",
		},
		{
			name: "missing required label",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.app.kubernetes.io/name", Value: "api"},
			},
		},
		{
			name: "wrong label value",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.team", Value: "platform"},
			},
		},
		{
			name: "non-label field is ignored",
			fields: []logstorage.Field{
				{Name: "team", Value: "payments"},
			},
		},
		{
			name: "no built-in route",
			fields: []logstorage.Field{
				{Name: "kubernetes.pod_labels.component", Value: "legacy"},
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

func TestNextRotatedPathSkipsCompressedBackup(t *testing.T) {
	dir := t.TempDir()
	fw := &fileWriter{
		dir:             dir,
		filePrefix:      "api",
		rotatedExt:      ".log",
		useLocalTime:    true,
		compressRotated: true,
	}

	base := filepath.Join(dir, "api."+time.Now().Format("20060102-150405.000")+".log.gz")
	if err := os.WriteFile(base, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got := fw.nextRotatedPathLocked()
	if got == base {
		t.Fatalf("nextRotatedPathLocked() reused compressed backup %q", base)
	}
}

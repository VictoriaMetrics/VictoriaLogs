package remotewrite

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestLoadTransforms(t *testing.T) {
	f := func(s string) {
		t.Helper()

		flush := func(lr *logstorage.LogRows) {
			panic(fmt.Errorf("BUG: should not be called"))
		}

		_, err := loadTransforms(s, flush)
		if err != nil {
			t.Fatalf("cannot load transforms %q: %s", s, err)
		}
	}

	// Inline.
	f(`inline: keep foo, bar;`)
	// Quoted inline.
	f(`inline:"keep foo, bar;"`)

	// HTTP URL.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `keep foo, bar;`)
	}))
	f(server.URL)
	server.Close()

	// File path.
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "1.vlt")
	if err := os.WriteFile(filePath, []byte("keep foo,bar;\n"), 0644); err != nil {
		t.Fatalf("cannot create temporary file %q: %s", filePath, err)
	}
	f(filePath)

	// Glob pattern.
	globPattern := filepath.Join(tmpDir, "*.vlt")
	f(globPattern)
}

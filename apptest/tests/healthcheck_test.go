package tests

import (
	"net"
	"os/exec"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/VictoriaMetrics/VictoriaLogs/apptest"
)

func TestVlsingleHealthcheck(t *testing.T) {
	fs.MustRemoveDir(t.Name())
	tc := apptest.NewTestCase(t)
	defer tc.Stop()

	sut := tc.MustStartVlsingle("vlsingle", nil)
	addr := sut.HTTPAddr()

	// The healthcheck must exit with 0 when the server is healthy.
	out, err := exec.Command("../../bin/victoria-logs-race", "-healthcheck", "-httpListenAddr="+addr).CombinedOutput()
	if err != nil {
		t.Fatalf("healthcheck against %s must exit with 0 status code; got error: %s; output: %q", addr, err, out)
	}

	// The healthcheck must exit with 1 when the server is unavailable.
	// The listener is kept open, so the probe cannot succeed against this address,
	// but the port cannot be taken over by another process either.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot open listener for the unavailable server address: %s", err)
	}
	defer ln.Close()
	deadAddr := ln.Addr().String()
	cmd := exec.Command("../../bin/victoria-logs-race", "-healthcheck", "-httpListenAddr="+deadAddr)
	out, err = cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("healthcheck against %s must exit with non-zero status code; output: %q", deadAddr, out)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok || exitErr.ExitCode() != 1 {
		t.Fatalf("healthcheck against %s must exit with 1 status code; got error: %s; output: %q", deadAddr, err, out)
	}
}

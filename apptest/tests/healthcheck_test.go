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

	addr := getFreeTCPAddr(t)
	tc.MustStartVlsingle("vlsingle", []string{"-httpListenAddr=" + addr})

	// The healthcheck must exit with 0 when the server is healthy.
	out, err := exec.Command("../../bin/victoria-logs-race", "-healthcheck", "-httpListenAddr="+addr).CombinedOutput()
	if err != nil {
		t.Fatalf("healthcheck against %s must exit with 0 status code; got error: %s; output: %q", addr, err, out)
	}

	// The healthcheck must exit with 1 when the server is unavailable.
	deadAddr := getFreeTCPAddr(t)
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

// getFreeTCPAddr returns a TCP address on 127.0.0.1 with a free port.
func getFreeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot obtain free TCP address: %s", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("cannot close listener at %s: %s", addr, err)
	}
	return addr
}

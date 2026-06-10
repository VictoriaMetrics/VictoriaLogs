package tests

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// check if necessary binaries are there.
	checkBinaryRequirement("../../bin/vlagent-race")
	checkBinaryRequirement("../../bin/victoria-logs-race")
	checkBinaryRequirement("../../bin/vlogscli-race")

	// start the integration test.
	os.Exit(m.Run())
}

// checkBinaryRequirement panic if required binary not exist.
func checkBinaryRequirement(path string) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			panic(fmt.Sprintf("integration test failed: %s not found. please run `make integration-test` to execute integration tests. check how different tests are executed: https://docs.victoriametrics.com/victoriametrics/contributing/#testing", path))
		}
	}
}

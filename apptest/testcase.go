package apptest

import (
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/google/go-cmp/cmp"
)

// TestCase holds the state and defines clean-up procedure common for all test
// cases.
type TestCase struct {
	t   *testing.T
	cli *Client

	startedApps map[string]Stopper
}

// Stopper is an interface of objects that needs to be stopped via Stop() call
type Stopper interface {
	Stop()
}

// NewTestCase creates a new test case.
func NewTestCase(t *testing.T) *TestCase {
	t.Parallel()
	return &TestCase{t, NewClient(), make(map[string]Stopper)}
}

// T returns the test state.
func (tc *TestCase) T() *testing.T {
	return tc.t
}

// Dir returns the directory name that should be used by as the -storageDataDir.
func (tc *TestCase) Dir() string {
	return tc.t.Name()
}

// Client returns an instance of the client that can be used for interacting with
// the app(s) under test.
func (tc *TestCase) Client() *Client {
	return tc.cli
}

// Stop performs the test case clean up, such as closing all client connections
// and removing the -storageDataDir directory.
//
// Note that the -storageDataDir is not removed in case of test case failure to
// allow for further manual debugging.
func (tc *TestCase) Stop() {
	tc.cli.CloseConnections()
	for _, app := range tc.startedApps {
		app.Stop()
	}
	if !tc.t.Failed() {
		fs.MustRemoveDir(tc.Dir())
	}
}

func (tc *TestCase) addApp(instance string, app Stopper) {
	if _, alreadyStarted := tc.startedApps[instance]; alreadyStarted {
		tc.t.Fatalf("%s has already been started", instance)
	}
	tc.startedApps[instance] = app
}

// StopApp stops the app identified by the `instance` name and removes it from
// the collection of started apps.
func (tc *TestCase) StopApp(instance string) {
	if app, exists := tc.startedApps[instance]; exists {
		app.Stop()
		delete(tc.startedApps, instance)
	}
}

// AssertOptions hold the assertion params, such as got and wanted values as
// well as the message that should be included into the assertion error message
// in case of failure.
//
// In VictoriaMetrics (especially the cluster version) the inserted data does
// not become visible for querying right away. Therefore, the first comparisons
// may fail. AssertOptions allow to configure how many times the actual result
// must be retrieved and compared with the expected one and for long to wait
// between the retries. If these two params (`Retries` and `Period`) are not
// set, the default values will be used.
//
// If it is known that the data is available, then the retry functionality can
// be disabled by setting the `DoNotRetry` field.
//
// AssertOptions are used by the TestCase.Assert() method, and this method uses
// cmp.Diff() from go-cmp package for comparing got and wanted values.
// AssertOptions, therefore, allows to pass cmp.Options to cmp.Diff() via
// `CmpOpts` field.
//
// Finally the `FailNow` field controls whether the assertion should fail using
// `testing.T.Errorf()` or `testing.T.Fatalf()`.
type AssertOptions struct {
	Msg        string
	Got        func() any
	Want       any
	CmpOpts    []cmp.Option
	DoNotRetry bool
	Retries    int
	Period     time.Duration
	FailNow    bool
}

// Assert compares the actual result with the expected one possibly multiple
// times in order to account for the fact that the inserted data does not become
// available for querying right away (especially in cluster version of
// VictoriaMetrics).
func (tc *TestCase) Assert(opts *AssertOptions) {
	tc.t.Helper()

	const (
		defaultRetries = 20
		defaultPeriod  = 100 * time.Millisecond
	)

	if opts.DoNotRetry {
		opts.Retries = 1
		opts.Period = 0
	} else {
		if opts.Retries <= 0 {
			opts.Retries = defaultRetries
		}
		if opts.Period <= 0 {
			opts.Period = defaultPeriod
		}
	}

	var diff string

	for range opts.Retries {
		diff = cmp.Diff(opts.Want, opts.Got(), opts.CmpOpts...)
		if diff == "" {
			return
		}
		time.Sleep(opts.Period)
	}

	msg := fmt.Sprintf("%s (-want, +got):\n%s", opts.Msg, diff)

	if opts.FailNow {
		tc.t.Fatal(msg)
	} else {
		tc.t.Error(msg)
	}
}

// MustStartDefaultVlsingle is a test helper function that starts an instance of
// vlsingle with defaults suitable for most tests.
func (tc *TestCase) MustStartDefaultVlsingle() *Vlsingle {
	tc.t.Helper()

	return tc.MustStartVlsingle("vlsingle", []string{
		"-storageDataPath=" + tc.Dir() + "/vlsingle",
		"-retentionPeriod=100y",
	})
}

// MustStartVlsingle is a test helper function that starts an instance of
// vlsingle and fails the test if the app fails to start.
func (tc *TestCase) MustStartVlsingle(instance string, flags []string) *Vlsingle {
	tc.t.Helper()

	app, err := StartVlsingle(instance, flags, tc.cli)
	if err != nil {
		tc.t.Fatalf("Could not start %s: %v", instance, err)
	}
	tc.addApp(instance, app)
	return app
}

// MustStartDefaultVlagent is a test helper function that starts an instance of
// vlagent with defaults suitable for most tests.
func (tc *TestCase) MustStartDefaultVlagent(remoteWriteURLs []string) *Vlagent {
	tc.t.Helper()

	return tc.MustStartVlagent("vlagent", remoteWriteURLs, nil)
}

// MustStartVlagent is a test helper function that starts an instance of
// vlagent and fails the test if the app fails to start.
func (tc *TestCase) MustStartVlagent(instance string, remoteWriteURLs []string, flags []string) *Vlagent {
	tc.t.Helper()

	app, err := StartVlagent(instance, remoteWriteURLs, flags, tc.cli)
	if err != nil {
		tc.t.Fatalf("Could not start %s: %v", instance, err)
	}
	tc.addApp(instance, app)
	return app
}

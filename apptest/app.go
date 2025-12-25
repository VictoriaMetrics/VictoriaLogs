package apptest

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// Regular expressions for runtime information to extract from the app logs.
var (
	httpListenAddrRE = regexp.MustCompile(`started server at http://(.*:\d{1,5})/`)

	logsStorageDataPathRE = regexp.MustCompile(`opening storage at -storageDataPath=(.*)`)
)

// app represents an instance of some VictoriaLogs server (such as vlsingle or vlagent).
type app struct {
	instance string
	binary   string
	flags    []string
	process  *os.Process
}

// mustStartApp starts an instance of an app using the app binary file path and flags.
//
// If the app has started successfully and all the requested items has been
// extracted from logs, the function returns the instance of the app and the
// extracted items. The extracted items are returned in the same order as the
// corresponding extract regular expression have been provided in the extractREs.
//
// The function exits with fatal error if the current process if the application
// has failed to startor the function has timed out extracting items from the
// log (normally because no log records match the regular expression).
func mustStartApp(t *testing.T, instance string, binary string, flags []string, extractREs []*regexp.Regexp) (*app, []string) {
	t.Helper()

	log.Printf("starting %s from %s with flags %s", instance, binary, flags)

	cmd := exec.Command(binary, flags...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("cannot obtain stdout for %s started from %s with flags %s: %s", instance, binary, flags, err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("cannot obtain stderr for %s started from %s with flags %s: %s", instance, binary, flags, err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start %s from %s with flags %s: %s", instance, binary, flags, err)
	}

	app := &app{
		instance: instance,
		binary:   binary,
		flags:    flags,
		process:  cmd.Process,
	}

	go app.processOutput("stdout", stdout, app.writeToStderr)

	lineProcessors := make([]lineProcessor, len(extractREs))
	reExtractors := make([]*reExtractor, len(extractREs))
	timeout := time.NewTimer(5 * time.Second).C
	for i, re := range extractREs {
		reExtractors[i] = newREExtractor(re, timeout)
		lineProcessors[i] = reExtractors[i].extractRE
	}
	go app.processOutput("stderr", stderr, append(lineProcessors, app.writeToStderr)...)

	extracts, err := extractREMatches(reExtractors, timeout)
	if err != nil {
		app.Stop()
		t.Fatalf("cannot extract %s from stdout and stderr for %s started from %s with flags %s: %s", extractREs, instance, binary, flags, err)
	}

	return app, extracts
}

// setDefaultFlags adds flags with default values to `flags` if it does not initially contain them.
func setDefaultFlags(flags []string, defaultFlags map[string]string) []string {
	var flagNames []string
	for _, f := range flags {
		if !strings.HasPrefix(f, "-") {
			panic(fmt.Sprintf("BUG: flag must start with '-'; got %q", f))
		}
		n := strings.IndexByte(f, '=')
		if n < 0 {
			panic(fmt.Sprintf("BUG: cannot find '=' in the flag %q", f))
		}
		flagName := f[:n]
		flagNames = append(flagNames, flagName)
	}

	for name, value := range defaultFlags {
		if !strings.HasPrefix(name, "-") {
			panic(fmt.Sprintf("BUG: default flag name must start with '-'; got %q", name))
		}
		if !slices.Contains(flagNames, name) {
			flags = append(flags, fmt.Sprintf("%s=%s", name, value))
		}
	}
	return flags
}

// Stop sends the app process a SIGINT signal and waits until it terminates
// gracefully.
func (app *app) Stop() {
	if err := app.process.Signal(os.Interrupt); err != nil {
		log.Fatalf("Could not send SIGINT signal to %s process: %v", app.instance, err)
	}
	if _, err := app.process.Wait(); err != nil {
		log.Fatalf("Could not wait for %s process completion: %v", app.instance, err)
	}
}

// Name returns the application instance name.
func (app *app) Name() string {
	return app.instance
}

// String returns the string representation of the app state.
func (app *app) String() string {
	return fmt.Sprintf("{instance: %q binary: %q flags: %q}", app.instance, app.binary, app.flags)
}

// lineProcessor is a function that is applied to the each line of the app
// output (stdout or stderr). The function returns true to indicate the caller
// that it has completed its work and should not be called again.
type lineProcessor func(line string) (done bool)

// processOutput invokes a set of processors on each line of app output (stdout
// or stderr). Once a line processor is done (returns true) it is never invoked
// again.
//
// A simple use case for this is to pipe the output of the child process to the
// output of the parent process. A more sophisticated one is to retrieve some
// runtime information from the child process logs, such as the server's
// host:port.
func (app *app) processOutput(outputName string, output io.Reader, lps ...lineProcessor) {
	activeLPs := map[int]lineProcessor{}
	for i, lp := range lps {
		activeLPs[i] = lp
	}

	scanner := bufio.NewScanner(output)
	for scanner.Scan() {
		line := scanner.Text()
		for i, process := range activeLPs {
			if process(line) {
				delete(activeLPs, i)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("could not scan %s %s: %v", app.instance, outputName, err)
	}
}

// writeToStderr is a line processor that writes the line to the stderr.
// The function always returns false to indicate its caller that each line must
// be written to the stderr.
func (app *app) writeToStderr(line string) bool {
	fmt.Fprintf(os.Stderr, "%s %s\n", app.instance, line)
	return false
}

// extractREMatches waits until all reExtractors return the result and then returns
// the combined result with items ordered the same way as reExtractors.
//
// The function returns an error if timeout occurs sooner then all reExtractors
// finish its work.
func extractREMatches(reExtractors []*reExtractor, timeout <-chan time.Time) ([]string, error) {
	n := len(reExtractors)
	notFoundREs := make(map[int]string)
	extracts := make([]string, n)
	cases := make([]reflect.SelectCase, n+1)
	for i, x := range reExtractors {
		cases[i] = x.selectCase
		notFoundREs[i] = x.re.String()
	}
	cases[n] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(timeout),
	}

	for notFound := n; notFound > 0; {
		i, value, _ := reflect.Select(cases)
		if i == n {
			// n-th select case means timeout.

			values := func(m map[int]string) []string {
				s := []string{}
				for _, v := range m {
					s = append(s, v)
				}
				return s
			}
			return nil, fmt.Errorf("could not extract some or all regexps from stderr: %q", values(notFoundREs))
		}
		extracts[i] = value.String()
		delete(notFoundREs, i)
		notFound--
	}
	return extracts, nil
}

// reExtractor extracts some information based on a regular expression from the
// app output within a timeout.
type reExtractor struct {
	re         *regexp.Regexp
	result     chan string
	timeout    <-chan time.Time
	selectCase reflect.SelectCase
}

// newREExtractor create a new reExtractor based on a regexp and a timeout.
func newREExtractor(re *regexp.Regexp, timeout <-chan time.Time) *reExtractor {
	result := make(chan string)
	return &reExtractor{
		re:      re,
		result:  result,
		timeout: timeout,
		selectCase: reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(result),
		},
	}
}

// extractRE is a line processor that extracts some information from a line
// based on a regular expression. The function returns true to indicate that
// it should not be called again, either when the match is found or due to
// the timeout. The found match is written to the x.result channel and it is
// important that this channel is monitored by a separate goroutine, otherwise
// the function will block.
func (x *reExtractor) extractRE(line string) bool {
	submatch := x.re.FindSubmatch([]byte(line))
	if len(submatch) > 0 {
		// Some regexps are used to just find a match without submatches.
		result := ""
		if len(submatch) > 1 {
			// But if submatches have been found, return the first one.
			result = string(submatch[1])
		}
		select {
		case x.result <- result:
		case <-x.timeout:
		}
		return true
	}
	return false
}

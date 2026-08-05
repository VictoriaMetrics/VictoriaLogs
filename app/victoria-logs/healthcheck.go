package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

var (
	healthcheck = flag.Bool("healthcheck", false, "Performs a healthcheck of the locally running VictoriaLogs by issuing a GET request to the /health endpoint at the first -httpListenAddr and exits with 0 status code if the response status code is 2xx, otherwise it exits with 1. This flag is intended to be used by the HEALTHCHECK instruction in Docker images, so the container health can be inspected with tools such as docker ps without having curl or wget inside the image. The healthcheck resolves its flags from the command line and, since the built-in HEALTHCHECK in Docker images runs with -envflag.enable, from environment variables; the running server only reads environment variables if it is started with -envflag.enable, so the healthcheck and the server must be started with matching flags, otherwise the healthcheck may probe a wrong address. The flag cannot be used if -httpListenAddr.useProxyProtocol is enabled for the first -httpListenAddr")
)

const (
	healthcheckTimeout = 3 * time.Second

	healthcheckHint = "hint: the healthcheck probes the address configured via its own flags; if VictoriaLogs listens on another address, " +
		"then pass the corresponding flags to -healthcheck or set the matching environment variables, e.g. httpListenAddr=:9430, " +
		"and start both the server and the healthcheck with -envflag.enable (the built-in HEALTHCHECK in Docker images already does this)"
)

// runHealthcheck performs a healthcheck of the locally running VictoriaLogs and returns the exit code.
func runHealthcheck() int {
	addr, err := getFirstHTTPListenAddr()
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot perform healthcheck: %s\n", err)
		return 1
	}
	if len(*useProxyProtocol) > 0 && (*useProxyProtocol)[0] {
		fmt.Fprintf(os.Stderr, "cannot perform healthcheck: -httpListenAddr.useProxyProtocol is enabled for %s\n", addr)
		return 1
	}
	healthURL, err := httpAddrToHealthURL(addr, httpserver.IsTLS(0), httpserver.GetPathPrefix())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot perform healthcheck: %s\n", err)
		return 1
	}
	cli := &http.Client{
		Timeout: healthcheckTimeout,
	}
	if strings.HasPrefix(healthURL, "https://") {
		cli.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	resp, err := cli.Get(healthURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck failed for %s: %s\n%s\n", healthURL, err, healthcheckHint)
		return 1
	}
	if err := resp.Body.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "cannot close response body for %s: %s\n", healthURL, err)
		return 1
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "healthcheck failed for %s: unexpected status code %d\n%s\n", healthURL, resp.StatusCode, healthcheckHint)
		return 1
	}
	fmt.Printf("OK\n")
	return 0
}

// getFirstHTTPListenAddr returns the first -httpListenAddr value or the default one.
func getFirstHTTPListenAddr() (string, error) {
	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		// The default -httpListenAddr value used by main.go; keep in sync with it.
		return ":9428", nil
	}
	addr := listenAddrs[0]
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", fmt.Errorf("cannot parse -httpListenAddr=%q: %w", addr, err)
	}
	return addr, nil
}

// httpAddrToHealthURL returns the URL for the /health endpoint served at the given -httpListenAddr.
func httpAddrToHealthURL(addr string, isTLS bool, pathPrefix string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("cannot parse listen addr %q: %w", addr, err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	scheme := "http"
	if isTLS {
		scheme = "https"
	}
	prefix := strings.TrimSuffix(pathPrefix, "/")
	return fmt.Sprintf("%s://%s%s/health", scheme, net.JoinHostPort(host, port), prefix), nil
}

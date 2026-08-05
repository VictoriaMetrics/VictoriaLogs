package main

import (
	"context"
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

var healthcheck = flag.Bool("healthcheck", false, "Whether to perform a healthcheck of the locally running VictoriaLogs by querying the /health endpoint "+
	"at the first -httpListenAddr, exiting with 0 status code on 2xx responses, otherwise with 1")

// v2 header with UNSPEC family and LOCAL command, used by load balancers for health checks
var proxyV2Header = []byte("\r\n\r\n\x00\r\nQUIT\n\x20\x00\x00\x00")

func runHealthcheck(addr string) int {
	url := healthcheckURL(addr, httpserver.IsTLS(0), httpserver.GetPathPrefix())
	// InsecureSkipVerify is needed for a local server with self-signed TLS cert
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	if useProxyProtocol.GetOptionalArg(0) {
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := new(net.Dialer).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if _, err := conn.Write(proxyV2Header); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		}
	}
	cli := &http.Client{Timeout: 3 * time.Second, Transport: tr}
	resp, err := cli.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot perform healthcheck for %s: %s\n", url, err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "unexpected response status %d from %s\n", resp.StatusCode, url)
		return 1
	}
	return 0
}

func healthcheckURL(addr string, isTLS bool, pathPrefix string) string {
	host, port, _ := net.SplitHostPort(addr)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	scheme := "http"
	if isTLS {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s%s/health", scheme, net.JoinHostPort(host, port), strings.TrimSuffix(pathPrefix, "/"))
}

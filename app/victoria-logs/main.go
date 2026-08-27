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

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/flagutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/procutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/pushmetrics"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
)

var (
	httpListenAddrs  = flagutil.NewArrayString("httpListenAddr", "TCP address to listen for incoming http requests. See also -httpListenAddr.useProxyProtocol")
	useProxyProtocol = flagutil.NewArrayBool("httpListenAddr.useProxyProtocol", "Whether to use proxy protocol for connections accepted at the given -httpListenAddr . "+
		"See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt . "+
		"With enabled proxy protocol http server cannot serve regular /metrics endpoint. Use -pushmetrics.url for metrics pushing")
	healthCheck = flag.Bool("health", false, "Whether to run a quick health check on the /health endpoint of a VictoriaLogs instance and exit. "+
		"These flags should be set if they are also set by the checked instance: -http.pathPrefix, -httpListenAddr (only first address is used), -httpListenAddr.useProxyProtocol, -tls")
)

func main() {
	// Write flags and help message to stdout, since it is easier to grep or pipe.
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = usage
	envflag.Parse()
	buildinfo.Init()
	initSecretFlags()
	logger.Init()

	listenAddrs := *httpListenAddrs
	if len(listenAddrs) == 0 {
		listenAddrs = []string{":9428"}
	}

	if *healthCheck {
		os.Exit(runHealthCheck(listenAddrs[0]))
	}

	logger.Infof("starting VictoriaLogs at %q...", listenAddrs)
	startTime := time.Now()

	vlstorage.Init()
	vlselect.Init()

	insertutil.SetLogRowsStorage(&vlstorage.Storage{})
	vlinsert.Init()

	go httpserver.Serve(listenAddrs, requestHandler, httpserver.ServeOptions{
		UseProxyProtocol: useProxyProtocol,
	})
	logger.Infof("started VictoriaLogs in %.3f seconds; see https://docs.victoriametrics.com/victorialogs/", time.Since(startTime).Seconds())

	pushmetrics.Init()
	sig := procutil.WaitForSigterm()
	logger.Infof("received signal %s", sig)
	pushmetrics.Stop()

	logger.Infof("gracefully shutting down webservice at %q", listenAddrs)
	startTime = time.Now()
	if err := httpserver.Stop(listenAddrs); err != nil {
		logger.Fatalf("cannot stop the webservice: %s", err)
	}
	logger.Infof("successfully shut down the webservice in %.3f seconds", time.Since(startTime).Seconds())

	vlinsert.Stop()
	vlselect.Stop()
	vlstorage.Stop()

	logger.Infof("the VictoriaLogs has been stopped in %.3f seconds", time.Since(startTime).Seconds())
}

func requestHandler(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/" {
		if r.Method != http.MethodGet {
			return false
		}
		w.Header().Add("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<h2>VictoriaLogs</h2></br>")
		fmt.Fprintf(w, "Version %s<br>", buildinfo.Version)
		fmt.Fprintf(w, "See docs at <a href='https://docs.victoriametrics.com/victorialogs/'>https://docs.victoriametrics.com/victorialogs/</a></br>")
		fmt.Fprintf(w, "Useful endpoints:</br>")
		httpserver.WriteAPIHelp(w, [][2]string{
			{"select/vmui", "Web UI for VictoriaLogs"},
			{"metrics", "available service metrics"},
			{"flags", "command-line flags"},
		})
		return true
	}
	if vlinsert.RequestHandler(w, r) {
		return true
	}
	if vlselect.RequestHandler(w, r) {
		return true
	}
	if vlstorage.RequestHandler(w, r) {
		return true
	}
	return false
}

func usage() {
	const s = `
victoria-logs is a log management and analytics service.

See the docs at https://docs.victoriametrics.com/victorialogs/
`
	flagutil.Usage(s)
}

// runHealthCheck makes a GET request to the /health endpoint of the given address.
func runHealthCheck(address string) int {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		logger.Errorf("failed to read address: %s", err)
		return 1
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	scheme := "http"
	if httpserver.IsTLS(0) {
		scheme = "https"
	}

	prefix := httpserver.GetPathPrefix()

	addr := fmt.Sprintf(
		"%s://%s%s/health", scheme, net.JoinHostPort(host, port), strings.TrimSuffix(prefix, "/"),
	)
	logger.Infof("sending health check request to %q", addr)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	if useProxyProtocol.GetOptionalArg(0) {
		transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			if _, err := conn.Write([]byte("\r\n\r\n\x00\r\nQUIT\n\x20\x00\x00\x00")); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		}
	}

	client := &http.Client{Timeout: 3 * time.Second, Transport: transport}

	res, err := client.Get(addr)
	if err != nil {
		logger.Errorf("health check request failed: %s", err)
		return 1
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		logger.Errorf("health check returned code %d", res.StatusCode)
		return 1
	}

	logger.Infof("health check successful")
	return 0
}

// initSecretFlags manage the default secret flags for victoria-logs application.
func initSecretFlags() {
	pushmetrics.InitSecretFlags()
	vlselect.InitSecretFlags()
}

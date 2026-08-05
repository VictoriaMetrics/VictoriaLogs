package main

import "testing"

func TestHTTPAddrToHealthURL(t *testing.T) {
	f := func(addr string, isTLS bool, pathPrefix, expectedURL string) {
		t.Helper()
		got, err := httpAddrToHealthURL(addr, isTLS, pathPrefix)
		if err != nil {
			t.Fatalf("unexpected error for addr=%q, isTLS=%v, pathPrefix=%q: %s", addr, isTLS, pathPrefix, err)
		}
		if got != expectedURL {
			t.Fatalf("unexpected url for addr=%q, isTLS=%v, pathPrefix=%q; got %q; want %q", addr, isTLS, pathPrefix, got, expectedURL)
		}
	}
	f(":9428", false, "", "http://127.0.0.1:9428/health")
	f("0.0.0.0:9428", false, "", "http://127.0.0.1:9428/health")
	f("[::]:9428", false, "", "http://127.0.0.1:9428/health")
	f("127.0.0.1:9428", false, "", "http://127.0.0.1:9428/health")
	f("localhost:9428", true, "", "https://localhost:9428/health")
	f(":9428", false, "/foo/bar", "http://127.0.0.1:9428/foo/bar/health")
	f(":9428", false, "/foo/bar/", "http://127.0.0.1:9428/foo/bar/health")
}

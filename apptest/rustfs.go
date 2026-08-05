package apptest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	RustfsRootUser     = "rustfsadmin"
	RustfsRootPassword = "rustfsadmin"
	RustfsBucket       = "vltest"
)

type Rustfs struct {
	process  *os.Process
	endpoint string
}

func TryStartRustfs(t *testing.T, instance string) (*Rustfs, bool) {
	t.Helper()

	binary := "../../bin/rustfs"
	if _, err := os.Stat(binary); err != nil {
		return nil, false
	}

	dataPath := filepath.Join(t.Name(), instance)
	if err := os.MkdirAll(dataPath, 0o755); err != nil {
		t.Fatalf("cannot create RustFS data directory %q: %v", dataPath, err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot find free port for RustFS: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"RUSTFS_VOLUMES="+dataPath,
		"RUSTFS_ADDRESS="+addr,
		"RUSTFS_ACCESS_KEY="+RustfsRootUser,
		"RUSTFS_SECRET_KEY="+RustfsRootPassword,
		"RUSTFS_CONSOLE_ENABLE=false",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start RustFS from %q: %v", binary, err)
	}

	r := &Rustfs{
		process:  cmd.Process,
		endpoint: "http://" + addr,
	}
	t.Cleanup(r.Stop)

	healthURL := r.endpoint + "/health"
	httpClient := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatalf("RustFS at %q didn't become ready in time", r.endpoint)
	}

	// Set AWS credentials so VictoriaLogs processes started in this test
	// can authenticate with RustFS. Must be called before t.Parallel().
	t.Setenv("AWS_ACCESS_KEY_ID", RustfsRootUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", RustfsRootPassword)

	r.mustCreateBucket(t)

	return r, true
}

// mustCreateBucket creates the bucket used for offloading. Unlike MinIO's
// legacy filesystem backend, RustFS doesn't expose a pre-created data
// subdirectory as a bucket automatically, so it must be created via the S3
// API.
func (r *Rustfs) mustCreateBucket(t *testing.T) {
	t.Helper()

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(r.endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(RustfsRootUser, RustfsRootPassword, ""),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(RustfsBucket),
	}); err != nil {
		t.Fatalf("cannot create RustFS bucket %q: %v", RustfsBucket, err)
	}
}

func (r *Rustfs) Stop() {
	r.process.Signal(os.Interrupt) //nolint:errcheck
	r.process.Wait()               //nolint:errcheck
}

func (r *Rustfs) Endpoint() string {
	return r.endpoint
}

func (r *Rustfs) OffloadDestination() string {
	return fmt.Sprintf("s3://%s", RustfsBucket)
}

func (r *Rustfs) OffloadFlags() []string {
	return []string{
		fmt.Sprintf("-offload.destination=%s", r.OffloadDestination()),
		fmt.Sprintf("-offload.s3.endpoint=%s", r.Endpoint()),
		"-offload.s3.forcePathStyle=true",
		"-offload.s3.region=us-east-1",
		"-offloadPeriod=24h",
	}
}

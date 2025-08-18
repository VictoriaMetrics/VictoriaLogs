package kubernetescollector

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

type kubeAPIConfig struct {
	Server        string
	BearerToken   string
	ClientCert    []byte
	ClientCertKey []byte
	GetCACert     func() (*x509.CertPool, error)
}

type kubeAPIClient struct {
	config *kubeAPIConfig
	c      *http.Client

	apiURL *url.URL
}

func newKubeAPIClient(cfg *kubeAPIConfig) (*kubeAPIClient, error) {
	certPool, err := cfg.GetCACert()
	if err != nil {
		return nil, fmt.Errorf("cannot get CA certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}
	if len(cfg.ClientCert) != 0 {
		clientCert, err := tls.X509KeyPair(cfg.ClientCert, cfg.ClientCertKey)
		if err != nil {
			return nil, fmt.Errorf("cannot load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}
	// todo: ca cert can be updated, so we need to reload it periodically
	c := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	apiURL, err := url.Parse(cfg.Server)
	if err != nil {
		return nil, fmt.Errorf("cannot parse server URL %q: %w", cfg.Server, err)
	}

	return &kubeAPIClient{
		config: cfg,
		c:      c,
		apiURL: apiURL,
	}, nil
}

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// watchNodePods starts watching Pod changes on the specified node.
// It returns a stream of watchEvent values representing updates to those Pods.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#watch-pod-v1-core
func (c *kubeAPIClient) watchNodePods(ctx context.Context, nodeName string) (podWatchStream, error) {
	args := url.Values{
		"watch": []string{"true"},
		"fieldSelector": []string{
			// Watch pods only on the given node.
			// See https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/
			"spec.nodeName=" + nodeName,
		},
	}

	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/pods", args)
	resp, err := c.c.Do(req)
	if err != nil {
		return podWatchStream{}, fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		_ = resp.Body.Close()
		return podWatchStream{}, fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	return podWatchStream{r: resp.Body}, nil
}

type podWatchStream struct {
	r io.ReadCloser
}

func (er podWatchStream) readEvents(h func(event watchEvent)) error {
	lr := newLineReader(er.r)
	for {
		line, err := lr.readLine()
		if err != nil {
			return fmt.Errorf("cannot read event line: %w", err)
		}

		var e watchEvent
		if err := json.Unmarshal(line, &e); err != nil {
			return fmt.Errorf("cannot decode event %q: %w", line, err)
		}
		h(e)
	}
}

func (er podWatchStream) close() error {
	return er.r.Close()
}

type pod struct {
	Metadata podMetadata `json:"metadata"`
	Status   podStatus   `json:"status"`
	Spec     podSpec     `json:"spec"`
}

type podMetadata struct {
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Namespace string            `json:"namespace"`
	UID       string            `json:"uid"`
}

type podSpec struct {
	NodeName       string         `json:"nodeName"`
	Containers     []podContainer `json:"containers"`
	InitContainers []podContainer `json:"initContainers"`
}

type podContainer struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type podStatus struct {
	Phase                 string            `json:"phase"`
	PodIP                 string            `json:"podIP"`
	ContainerStatuses     []containerStatus `json:"containerStatuses"`
	InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
	QosClass              string            `json:"qosClass"`
}

type containerStatus struct {
	Name        string `json:"name"`
	ContainerID string `json:"containerID"`
	Image       string `json:"image"`
}

func (ps *podStatus) findContainerStatus(containerName string) (containerStatus, bool) {
	for _, cs := range ps.ContainerStatuses {
		if cs.Name == containerName {
			return cs, true
		}
	}
	return containerStatus{}, false
}

func (ps *podStatus) findInitContainerStatus(containerName string) (containerStatus, bool) {
	for _, cs := range ps.InitContainerStatuses {
		if cs.Name == containerName {
			return cs, true
		}
	}
	return containerStatus{}, false
}

// getPod returns the pod with the given namespace and name.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#read-pod-v1-core
func (c *kubeAPIClient) getPod(ctx context.Context, namespace, podName string) (pod, error) {
	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/namespaces/"+namespace+"/pods/"+podName, nil)
	resp, err := c.c.Do(req)
	if err != nil {
		return pod{}, fmt.Errorf("cannot do /pods/<podName> request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		return pod{}, fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	var p pod
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return pod{}, fmt.Errorf("cannot decode response body: %w", err)
	}
	return p, nil
}

type nodeMetadata struct {
	Name string `json:"name"`
}

type node struct {
	Metadata nodeMetadata `json:"metadata"`
}

type nodeList struct {
	Items []node `json:"items"`
}

// getNodes returns the list of node names in the Kubernetes cluster.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#list-node-v1-core
func (c *kubeAPIClient) getNodes(ctx context.Context) ([]string, error) {
	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/nodes", nil)
	resp, err := c.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		return nil, fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	var nl nodeList
	if err := json.NewDecoder(resp.Body).Decode(&nl); err != nil {
		return nil, fmt.Errorf("cannot decode response body: %w", err)
	}

	var nodes []string
	for _, n := range nl.Items {
		nodes = append(nodes, n.Metadata.Name)
	}
	return nodes, nil
}

func (c *kubeAPIClient) mustCreateRequest(ctx context.Context, method, urlPath string, args url.Values) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, "/", nil)
	if err != nil {
		logger.Panicf("BUG: cannot create request: %w", err)
	}
	u := *c.apiURL
	req.URL = &u
	req.URL.Path = urlPath
	req.URL.RawQuery = args.Encode()
	if c.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.config.BearerToken)
	}
	return req
}

// lineReader is like bufio.Scanner but it can read lines of arbitrary length.
type lineReader struct {
	lr  *bufio.Reader
	buf []byte
}

func newLineReader(r io.Reader) lineReader {
	return lineReader{
		lr:  bufio.NewReaderSize(r, 16*1024),
		buf: []byte{},
	}
}

func (lr *lineReader) readLine() ([]byte, error) {
	lr.buf = lr.buf[:0]
	for {
		line, isPrefix, err := lr.lr.ReadLine()
		if err != nil {
			return nil, err
		}
		if isPrefix {
			// The line is incomplete, we need to read more data to finish it.
			lr.buf = append(lr.buf, line...)
			continue
		}

		if len(lr.buf) == 0 {
			// Fast path: the entire line fits within the *bufio.Reader, no need for further reading.
			return line, nil
		}

		// Slow path: the line doesn't fit in the buffer, so we append the remainder to the prefix and return the complete line.
		lr.buf = append(lr.buf, line...)
		return lr.buf, nil
	}
}

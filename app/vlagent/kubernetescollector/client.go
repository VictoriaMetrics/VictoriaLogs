package kubernetescollector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
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

	var clientCerts []tls.Certificate
	if len(cfg.ClientCert) != 0 {
		cc, err := tls.X509KeyPair(cfg.ClientCert, cfg.ClientCertKey)
		if err != nil {
			return nil, fmt.Errorf("cannot load client certificate: %w", err)
		}
		clientCerts = append(clientCerts, cc)
	}

	tr := httputil.NewTransport(false, "vlagent_kubernetescollector")
	tr.IdleConnTimeout = time.Minute
	tr.TLSHandshakeTimeout = time.Second * 15
	tr.TLSClientConfig.RootCAs = certPool
	tr.TLSClientConfig.Certificates = clientCerts

	// todo: ca cert can be updated, so we need to reload it periodically
	c := &http.Client{
		Transport: tr,
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
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#watch-pod-v1-core
//
// watchNodePods accepts the resourceVersion argument to skip already processed events.
// It is recommended to skip already processed events to significantly reduce the load on the Kubernetes API server.
// The resourceVersion value can be obtained from the podListMetadata.ResourceVersion field returned by getNodePods
// or from the watchEvent.Object.metadata.resourceVersion field.
// See https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes
func (c *kubeAPIClient) watchNodePods(ctx context.Context, nodeName, resourceVersion string) (podWatchStream, error) {
	args := url.Values{
		"watch": []string{"true"},
		"fieldSelector": []string{
			// Watch pods only on the given node.
			// See https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/
			"spec.nodeName=" + nodeName,
		},
	}

	// Set resourceVersion if it is non-empty to skip already processed events.
	if resourceVersion != "" {
		args["resourceVersion"] = []string{resourceVersion}
	}

	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/pods", args)
	resp, err := c.c.Do(req)
	if err != nil {
		return podWatchStream{}, fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}

	if resp.StatusCode == http.StatusGone && resourceVersion != "" {
		// Requested watch operation fail because the historical resourceVersion is too old.
		// Fallback to watching from the beginning.
		// See https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-watch
		return c.watchNodePods(ctx, nodeName, "")
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

func (pws podWatchStream) readEvents(h func(event watchEvent) error) error {
	d := json.NewDecoder(pws.r)
	for {
		var e watchEvent
		if err := d.Decode(&e); err != nil {
			return fmt.Errorf("cannot parse WatchEvent json response: %w", err)
		}
		if err := h(e); err != nil {
			return err
		}
	}
}

func (pws podWatchStream) close() error {
	return pws.r.Close()
}

type pod struct {
	Metadata podMetadata `json:"metadata"`
	Status   podStatus   `json:"status"`
	Spec     podSpec     `json:"spec"`
}

type podMetadata struct {
	Name            string            `json:"name"`
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	Namespace       string            `json:"namespace"`
	ResourceVersion string            `json:"resourceVersion"`
	UID             string            `json:"uid"`
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

type podList struct {
	Metadata podListMetadata `json:"metadata"`
	Items    []pod           `json:"items"`
}

type podListMetadata struct {
	ResourceVersion string `json:"resourceVersion"`
}

// getNodePods returns a list of pods on the given node.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#list-all-namespaces-pod-v1-core
func (c *kubeAPIClient) getNodePods(ctx context.Context, nodeName string) (podList, error) {
	args := url.Values{
		"fieldSelector": []string{
			// Watch pods only on the given node.
			// See https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/
			"spec.nodeName=" + nodeName,
		},
	}

	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/pods", args)
	resp, err := c.c.Do(req)
	if err != nil {
		return podList{}, fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		_ = resp.Body.Close()
		return podList{}, fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	var pl podList
	if err := json.NewDecoder(resp.Body).Decode(&pl); err != nil {
		return podList{}, fmt.Errorf("cannot decode response body: %w", err)
	}
	return pl, nil
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
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
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

// getNodes returns a node by its name.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#read-node-v1-core
func (c *kubeAPIClient) getNodeByName(ctx context.Context, nodeName string) (node, error) {
	req := c.mustCreateRequest(ctx, http.MethodGet, "/api/v1/nodes/"+nodeName, nil)
	resp, err := c.c.Do(req)
	if err != nil {
		return node{}, fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		return node{}, fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	var n node
	if err := json.NewDecoder(resp.Body).Decode(&n); err != nil {
		return node{}, fmt.Errorf("cannot decode response body: %w", err)
	}

	return n, nil
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

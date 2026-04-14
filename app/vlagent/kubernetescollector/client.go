package kubernetescollector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httputil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
)

type kubeAPIConfig struct {
	server string
	ac     *promauth.Config
}

type kubeAPIClient struct {
	config *kubeAPIConfig
	c      *http.Client

	apiURL *url.URL
}

func newKubeAPIClient(cfg *kubeAPIConfig) (*kubeAPIClient, error) {
	tr := httputil.NewTransport(false, "vlagent_kubernetescollector")
	tr.IdleConnTimeout = time.Minute
	tr.TLSHandshakeTimeout = time.Second * 15

	c := &http.Client{
		Transport: cfg.ac.NewRoundTripper(tr),
	}

	apiURL, err := url.Parse(cfg.server)
	if err != nil {
		return nil, fmt.Errorf("cannot parse server URL %q: %w", cfg.server, err)
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
	resp, err := c.sendRequest(req)
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

// podList represents a Kubernetes PodList object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#podlist-v1-core
type podList struct {
	Items    []pod    `json:"items"`
	Metadata listMeta `json:"metadata"`
}

// pod represents a Kubernetes Pod object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#pod-v1-core
type pod struct {
	Metadata objectMeta `json:"metadata"`
	Spec     podSpec    `json:"spec"`
	Status   podStatus  `json:"status"`
}

// objectMeta represents a Kubernetes ObjectMeta object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#objectmeta-v1-meta
type objectMeta struct {
	Name            string            `json:"name"`
	Labels          map[string]string `json:"labels"`
	Annotations     map[string]string `json:"annotations"`
	Namespace       string            `json:"namespace"`
	ResourceVersion string            `json:"resourceVersion"`
}

// podSpec represents a Kubernetes PodSpec object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#podspec-v1-core
type podSpec struct {
	NodeName       string         `json:"nodeName"`
	Containers     []podContainer `json:"containers"`
	InitContainers []podContainer `json:"initContainers"`
}

// podContainer represents a Kubernetes Container object.
// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#container-v1-core
type podContainer struct {
	Name string `json:"name"`
}

// podStatus represents a Kubernetes PodStatus object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#podstatus-v1-core
type podStatus struct {
	PodIP                 string            `json:"podIP"`
	ContainerStatuses     []containerStatus `json:"containerStatuses"`
	InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
}

// containerStatus represents a Kubernetes ContainerStatus object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#containerstatus-v1-core
type containerStatus struct {
	Name        string `json:"name"`
	ContainerID string `json:"containerID"`
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

// listMeta represents a Kubernetes ListMeta object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#listmeta-v1-meta
type listMeta struct {
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

	var pl podList
	err := c.readResourceGeneric(ctx, "/api/v1/pods", args, &pl)
	return pl, err
}

// getPod returns the pod with the given namespace and name.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#read-pod-v1-core
func (c *kubeAPIClient) getPod(ctx context.Context, namespace, podName string) (pod, error) {
	var p pod
	err := c.readResourceGeneric(ctx, "/api/v1/namespaces/"+namespace+"/pods/"+podName, nil, &p)
	return p, err
}

// nodeList represents a Kubernetes NodeList object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#nodelist-v1-core
type nodeList struct {
	Items []node `json:"items"`
}

// node represents a Kubernetes Node object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#node-v1-core
type node struct {
	Metadata objectMeta `json:"metadata"`
}

// getNodes returns the list of node names in the Kubernetes cluster.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#list-node-v1-core
func (c *kubeAPIClient) getNodes(ctx context.Context) ([]string, error) {
	nl := nodeList{}
	if err := c.readResourceGeneric(ctx, "/api/v1/nodes", nil, &nl); err != nil {
		return nil, err
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
	var n node
	err := c.readResourceGeneric(ctx, "/api/v1/nodes/"+nodeName, nil, &n)
	return n, err
}

// namespaceList represents a Kubernetes NamespaceList object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#namespacelist-v1-core
type namespaceList struct {
	Items []namespace `json:"items"`
}

// namespace represents a Kubernetes Namespace object.
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#namespace-v1-core
type namespace struct {
	Metadata objectMeta `json:"metadata"`
}

// getNamespaces retrieves the list of namespaces in the Kubernetes cluster.
//
// See https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.26/#list-all-namespaces-cronjob-v1-batch
func (c *kubeAPIClient) getNamespaces(ctx context.Context) (namespaceList, error) {
	var nl namespaceList
	err := c.readResourceGeneric(ctx, "/api/v1/namespaces", nil, &nl)
	return nl, err
}

func (c *kubeAPIClient) readResourceGeneric(ctx context.Context, urlPath string, args url.Values, dst any) error {
	req := c.mustCreateRequest(ctx, http.MethodGet, urlPath, args)
	resp, err := c.sendRequest(req)
	if err != nil {
		return fmt.Errorf("cannot do %q GET request: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, err := io.ReadAll(resp.Body)
		if err != nil {
			payload = []byte(err.Error())
		}
		return fmt.Errorf("unexpected status code %d from %q; response: %q", resp.StatusCode, req.URL.String(), payload)
	}

	if err := json.NewDecoder(resp.Body).Decode(&dst); err != nil {
		return fmt.Errorf("cannot decode response body: %w", err)
	}
	return nil
}

func (c *kubeAPIClient) mustCreateRequest(ctx context.Context, method, urlPath string, args url.Values) *http.Request {
	req, err := http.NewRequestWithContext(ctx, method, "/", nil)
	if err != nil {
		logger.Panicf("BUG: cannot create request: %s", err)
	}
	u := *c.apiURL
	req.URL = &u
	req.URL.Path = urlPath
	req.URL.RawQuery = args.Encode()
	return req
}

func (c *kubeAPIClient) sendRequest(req *http.Request) (*http.Response, error) {
	if err := c.config.ac.SetHeaders(req, true); err != nil {
		return nil, err
	}

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

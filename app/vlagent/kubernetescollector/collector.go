package kubernetescollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/remotewrite"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type kubernetesCollector struct {
	client *kubeAPIClient

	currentNode node

	// namespaces caches metadata for all namespaces in the cluster.
	// namespacesLock guards concurrent access, since refreshNamespaces may run while events are handled.
	namespaces     map[string]namespace
	namespacesLock sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// logsPath is the path to the directory containing Kubernetes container logs.
	// This is typically /var/log/containers in standard Kubernetes deployments,
	// but may vary depending on the vlagent mount configuration.
	// This directory contains symlinks with specific filenames to actual files.
	logsPath string

	fileCollector *fileCollector
}

// startKubernetesCollector starts watching Kubernetes cluster on the given node and starts collecting container logs.
// The collector monitors container logs in the specified logsPath directory and uses checkpointsPath to track reading progress.
// The caller must call stop() when the kubernetesCollector is no longer needed.
func startKubernetesCollector(client *kubeAPIClient, currentNodeName, logsPath, checkpointsPath string, excludeFilter *logstorage.Filter) (*kubernetesCollector, error) {
	_, err := os.Stat(logsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot access logs dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	currentNode, err := client.getNodeByName(ctx, currentNodeName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot get information about current node %q: %w", currentNodeName, err)
	}

	kc := &kubernetesCollector{
		client:      client,
		currentNode: currentNode,
		ctx:         ctx,
		cancel:      cancel,
		logsPath:    logsPath,
		namespaces:  make(map[string]namespace),
	}

	nsl, err := client.getAllNamespaces(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot fetch namespaces: %w; this is required for proper filtering", err)
	}
	kc.setNamespaces(nsl.Items)

	storage := &remotewrite.Storage{}
	newProcessor := func(commonFields []logstorage.Field) processor {
		return newLogFileProcessor(storage, commonFields)
	}
	fc := startFileCollector(checkpointsPath, excludeFilter, newProcessor)
	kc.fileCollector = fc

	pl, err := client.getNodePods(ctx, currentNodeName)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot get Pods on node %q: %w", currentNodeName, err)
	}

	// Start reading existing Pod logs.
	for _, pod := range pl.Items {
		kc.startReadPodLogs(pod)
	}
	// Cleanup checkpoints for deleted Pods.
	fc.cleanupCheckpoints()

	// Start watching for new Pods.
	kc.startWatchCluster(kc.ctx, pl.Metadata.ResourceVersion)

	return kc, nil
}

// startWatchCluster starts watching Pods scheduled on the given node.
// It calls handleUpdateEvent for each received event.
func (kc *kubernetesCollector) startWatchCluster(ctx context.Context, resourceVersion string) {
	handleEvent := func(event watchEvent) {
		switch event.Type {
		case "ADDED", "MODIFIED":
			var pod pod
			if err := json.Unmarshal(event.Object, &pod); err != nil {
				logger.Panicf("FATAL: cannot unmarshal Kubernetes event object %q: %s", event.Object, err)
			}

			kc.startReadPodLogs(pod)

			// Update resourceVersion to the latest seen.
			resourceVersion = pod.Metadata.ResourceVersion
		case "DELETED":
			// Ignore deleted pods.
		case "ERROR":
			logger.Errorf("got an error event from Kubernetes API: %q", string(event.Object))
		default:
			logger.Errorf("unexpected Kubernetes event type %q: %s", event.Type, string(event.Object))
		}
	}

	currentNodeName := kc.currentNode.Metadata.Name

	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()

		stopCh := ctx.Done()
		bt := newBackoffTimer(time.Millisecond*200, time.Second*30)
		defer bt.stop()

		errorFired := false

		lastEOF := time.Time{}

		for {
			r, err := kc.client.watchNodePods(ctx, currentNodeName, resourceVersion)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				errorFired = true

				logger.Errorf("failed to start watching Pods on node %q: %s; will retry in %s", currentNodeName, err, bt.currentDelay())
				bt.wait(stopCh)
				continue
			}

			if errorFired {
				logger.Infof("successfully re-established watching Pods on node %q", currentNodeName)
			}
			errorFired = false

			bt.reset()

			err = r.readEvents(handleEvent)
			_ = r.close()
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				isEOF := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
				if isEOF && time.Since(lastEOF) > time.Minute {
					// Kubernetes API server closed the connection.
					// This is expected to happen from time to time.
					// Ignore EOF errors happening not more often than once per minute.
					lastEOF = time.Now()
					continue
				}

				logger.Errorf("failed to read the Kubernetes Pod watch stream: %s", err)
				errorFired = true
				continue
			}
		}
	}()
}

func (kc *kubernetesCollector) startReadPodLogs(pod pod) {
	ns := kc.getNamespace(pod.Metadata.Namespace)

	startRead := func(pc podContainer, cs containerStatus) {
		commonFields := getCommonFields(kc.currentNode, ns, pod, cs)

		filePath := kc.getLogFilePath(pod, pc, cs)

		kc.fileCollector.startRead(filePath, commonFields)
	}

	for _, pc := range pod.Spec.Containers {
		cs, ok := pod.Status.findContainerStatus(pc.Name)
		if !ok || cs.ContainerID == "" {
			// Container in the pod is not running.
			continue
		}
		startRead(pc, cs)
	}

	for _, pc := range pod.Spec.InitContainers {
		cs, ok := pod.Status.findInitContainerStatus(pc.Name)
		if !ok || cs.ContainerID == "" {
			// Container in the pod is not running.
			continue
		}
		startRead(pc, cs)
	}
}

// streamFieldNames is a list of _stream fields.
// Must be synced with getCommonFields.
var streamFieldNames = []string{"kubernetes.container_name", "kubernetes.pod_name", "kubernetes.pod_namespace"}

func getCommonFields(n node, ns namespace, p pod, cs containerStatus) []logstorage.Field {
	var fs logstorage.Fields

	// Fields should match vector.dev kubernetes_source for easy migration.
	fs.Add("kubernetes.container_name", cs.Name)
	fs.Add("kubernetes.pod_name", p.Metadata.Name)
	fs.Add("kubernetes.pod_namespace", p.Metadata.Namespace)
	fs.Add("kubernetes.container_id", cs.ContainerID)
	fs.Add("kubernetes.pod_ip", p.Status.PodIP)
	fs.Add("kubernetes.pod_node_name", p.Spec.NodeName)

	for k, v := range p.Metadata.Labels {
		fieldName := "kubernetes.pod_labels." + k
		fs.Add(fieldName, v)
	}
	for k, v := range p.Metadata.Annotations {
		fieldName := "kubernetes.pod_annotations." + k
		fs.Add(fieldName, v)
	}

	for k, v := range ns.Metadata.Labels {
		fieldName := "kubernetes.namespace_labels." + k
		fs.Add(fieldName, v)
	}
	for k, v := range ns.Metadata.Annotations {
		fieldName := "kubernetes.namespace_annotations." + k
		fs.Add(fieldName, v)
	}

	for k, v := range n.Metadata.Labels {
		fieldName := "kubernetes.node_labels." + k
		fs.Add(fieldName, v)
	}
	for k, v := range n.Metadata.Annotations {
		fieldName := "kubernetes.node_annotations." + k
		fs.Add(fieldName, v)
	}

	return fs.Fields
}

func (kc *kubernetesCollector) getLogFilePath(p pod, pc podContainer, cs containerStatus) string {
	cid := cs.ContainerID
	// Trim the container runtime prefix from the container ID.
	// A container ID format has the form "docker://<container_id>" or "containerd://<container_id>".
	if n := strings.Index(cs.ContainerID, "://"); n >= 0 {
		cid = cs.ContainerID[n+len("://"):]
	}

	if p.Metadata.Name == "" || p.Metadata.Namespace == "" || pc.Name == "" || cid == "" {
		logger.Panicf("FATAL: got invalid container info from Kubernetes API: pod name %q, namespace %q, container name %q, container ID %q",
			p.Metadata.Name, p.Metadata.Namespace, pc.Name, cid)
	}

	filename := p.Metadata.Name + "_" + p.Metadata.Namespace + "_" + pc.Name + "-" + cid + ".log"
	logfilePath := path.Join(kc.logsPath, filename)
	return logfilePath
}

func (kc *kubernetesCollector) stop() {
	kc.cancel()
	kc.wg.Wait()
	kc.fileCollector.stop()
}

// setNamespaces replaces the cached namespaces map with the provided list.
func (kc *kubernetesCollector) setNamespaces(namespaces []namespace) {
	m := make(map[string]namespace, len(namespaces))
	for _, ns := range namespaces {
		m[ns.Metadata.Name] = ns
	}

	kc.namespacesLock.Lock()
	kc.namespaces = m
	kc.namespacesLock.Unlock()
}

// getNamespace returns namespace metadata from cache.
// If the namespace is not in cache (e.g., it was created after vlagent startup),
// this method refreshes the namespace list from Kubernetes API.
// It terminates vlagent if the namespace cannot be found or if the API call fails,
// as processing logs without complete namespace metadata could bypass excludeFilter.
func (kc *kubernetesCollector) getNamespace(namespaceName string) namespace {
	kc.namespacesLock.RLock()
	ns, ok := kc.namespaces[namespaceName]
	kc.namespacesLock.RUnlock()
	if ok {
		return ns
	}

	if err := kc.refreshNamespaces(); err != nil {
		logger.Fatalf("FATAL: cannot refresh namespaces: %s", err)
	}

	kc.namespacesLock.RLock()
	ns, ok = kc.namespaces[namespaceName]
	kc.namespacesLock.RUnlock()
	if !ok {
		logger.Fatalf("FATAL: cannot find namespace %q in Kubernetes API response", namespaceName)
	}
	return ns
}

// refreshNamespaces fetches all namespaces from Kubernetes API and updates the cache.
func (kc *kubernetesCollector) refreshNamespaces() error {
	nsl, err := kc.client.getAllNamespaces(kc.ctx)
	if err != nil {
		return fmt.Errorf("cannot fetch namespaces from Kubernetes API: %w", err)
	}

	kc.setNamespaces(nsl.Items)

	logger.Infof("refreshed namespace cache: %d namespaces", len(nsl.Items))
	return nil
}

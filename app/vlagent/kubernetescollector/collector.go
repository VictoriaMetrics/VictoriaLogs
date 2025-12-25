package kubernetescollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	}

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

	// Begin watching for new Pods and start reading their logs.
	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()
		kc.watchForPodsUpdates(ctx, pl.Metadata.ResourceVersion)
	}()

	return kc, nil
}

// watchForPodsUpdates starts watching Pods scheduled on the current node.
// It calls startReadPodLogs for each new or modified Pod.
func (kc *kubernetesCollector) watchForPodsUpdates(ctx context.Context, resourceVersion string) {
	currentNodeName := kc.currentNode.Metadata.Name

	bt := newBackoffTimer(time.Millisecond*200, time.Second*30)
	defer bt.stop()

	errorFired := false

	handleEvent := func(event watchEvent) error {
		switch event.Type {
		case "ADDED", "MODIFIED":
			bt.reset()

			if errorFired {
				logger.Infof("successfully re-established watching Pods on Node %q", currentNodeName)
			}
			errorFired = false

			var pod pod
			if err := json.Unmarshal(event.Object, &pod); err != nil {
				logger.Panicf("FATAL: cannot parse Kubernetes event object %q: %s", event.Object, err)
			}

			kc.startReadPodLogs(pod)

			// Update resourceVersion to the latest seen.
			resourceVersion = pod.Metadata.ResourceVersion
			return nil
		case "DELETED":
			// Ignore deleted pods.
			return nil
		case "ERROR":
			errorMessage := struct {
				Code int `json:"code"`
			}{}
			if err := json.Unmarshal(event.Object, &errorMessage); err != nil {
				logger.Panicf("FATAL: cannot parse Kubernetes error message %q: %s", event.Object, err)
			}

			if errorMessage.Code == http.StatusGone && resourceVersion != "" {
				// The resourceVersion is no longer valid, see: https://kubernetes.io/docs/reference/using-api/api-concepts/#410-gone-responses
				resourceVersion = ""
				return nil
			}

			return fmt.Errorf("unexpected error message: %q", event.Object)
		default:
			return fmt.Errorf("unexpected event type %q: %q", event.Type, event.Object)
		}
	}

	stopCh := ctx.Done()

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

			errorFired = true

			logger.Errorf("failed to read Pod events from the Kubernetes API: %s; will retry in %s", err, bt.currentDelay())
			bt.wait(stopCh)
			continue
		}
	}
}

func (kc *kubernetesCollector) startReadPodLogs(pod pod) {
	startRead := func(pc podContainer, cs containerStatus) {
		commonFields := getCommonFields(kc.currentNode, pod, cs)

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

func getCommonFields(n node, p pod, cs containerStatus) []logstorage.Field {
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

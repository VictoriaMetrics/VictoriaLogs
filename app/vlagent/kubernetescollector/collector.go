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

type client interface {
	// watchNodePods starts watching Pods scheduled on the given node.
	// The returned stream includes initial events representing the current state
	// of all Pods running on that node, followed by incremental updates.
	watchNodePods(ctx context.Context, node string) (podWatchStream, error)
}

type kubernetesCollector struct {
	client      client
	currentNode string

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
func startKubernetesCollector(client client, currentNode, logsPath, checkpointsPath string, excludeFilter *logstorage.Filter) (*kubernetesCollector, error) {
	_, err := os.Stat(logsPath)
	if err != nil {
		return nil, fmt.Errorf("cannot access logs dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &kubernetesCollector{
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
	c.fileCollector = fc

	c.startWatchCluster(c.ctx)

	return c, nil
}

// startWatchCluster starts watching Pods scheduled on the given node.
// It calls handleUpdateEvent for each received event.
func (kc *kubernetesCollector) startWatchCluster(ctx context.Context) {
	kc.wg.Add(1)
	go func() {
		defer kc.wg.Done()

		stopCh := ctx.Done()
		bt := newBackoffTimer(time.Millisecond*200, time.Second*30)
		defer bt.stop()

		errorFired := false

		lastEOF := time.Time{}

		for {
			r, err := kc.client.watchNodePods(ctx, kc.currentNode)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				errorFired = true

				logger.Errorf("failed to start watching Pods on node %q: %s; will retry in %s", kc.currentNode, err, bt.currentDelay())
				bt.wait(stopCh)
				continue
			}

			if errorFired {
				logger.Infof("successfully re-established watching Pods on node %q", kc.currentNode)
			}
			errorFired = false

			bt.reset()

			err = r.readEvents(kc.handleEvent)
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

func (kc *kubernetesCollector) handleEvent(event watchEvent) {
	switch event.Type {
	case "ADDED", "MODIFIED":
		kc.handleUpdateEvent(event.Object)
	case "DELETED":
		// ignore deleted pods
	case "ERROR":
		logger.Errorf("got an error event from Kubernetes API: %q", string(event.Object))
	default:
		logger.Errorf("unexpected Kubernetes event type %q: %s", event.Type, string(event.Object))
	}
}

// handleUpdateEvent prepares log file path and common fields for the given pod
// and delegates log file processing to fileCollector.
func (kc *kubernetesCollector) handleUpdateEvent(data json.RawMessage) {
	var pod pod
	if err := json.Unmarshal(data, &pod); err != nil {
		logger.Panicf("FATAL: cannot unmarshal Kubernetes event object %q: %s", data, err)
	}

	startRead := func(pc podContainer, cs containerStatus) {
		commonFields := getCommonFields(pod, cs)

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

func getCommonFields(p pod, cs containerStatus) []logstorage.Field {
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

package kubernetescollector

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

var (
	enabled         = flag.Bool("kubernetesCollector", true, "Whether to enable collecting logs from Kubernetes")
	checkpointsPath = flag.String("kubernetesCollector.checkpointsPath", "",
		"Path to file with checkpoints for Kubernetes logs. "+
			"Checkpoints are used to persist the read offsets for Kubernetes container logs. "+
			"When vlagent is restarted, it resumes reading logs from the stored offsets to avoid log duplication; "+
			"if this flag isn't set, then checkpoints are saved into vlagent-kubernetes-checkpoints.json under -tmpDataPath directory")
	logsPath = flag.String("kubernetesCollector.logsPath", "/var/log/containers",
		"Path to the directory with Kubernetes container logs (usually /var/log/containers). "+
			"This should point to the kubelet-managed directory containing symlinks to pod logs. "+
			"vlagent must have read access to this directory and to the target log files, typically located under /var/log/pods and /var/lib on the host")
	excludeFilter = flag.String("kubernetesCollector.excludeFilter", "", "Optional LogsQL filter for excluding container logs. "+
		"The filter is applied to container metadata fields (e.g., kubernetes.pod_namespace, kubernetes.container_name) before reading the log files. "+
		"This significantly reduces CPU and I/O usage by skipping logs from unwanted containers. "+
		"See https://docs.victoriametrics.com/victorialogs/vlagent/#filtering-kubernetes-logs")
	csiRbdHdd  = flag.Bool("kubernetesCollector.csiRbdHdd", false, "Whether to collect CSI RBD HDD logs from namespace rbd-hdd")
	csiRbdSsd  = flag.Bool("kubernetesCollector.csiRbdSsd", false, "Whether to collect CSI RBD SSD logs from namespace rbd-ssd")
	csiRbdNvme = flag.Bool("kubernetesCollector.csiRbdNvme", false, "Whether to collect CSI RBD NVMe logs from namespace rbd-nvme")
)

var collector *kubernetesCollector

func Init(tmpDataPath string) {
	if !*enabled {
		return
	}

	cfg, isLocal, err := loadKubeAPIConfig()
	if err != nil {
		logger.Fatalf("cannot load Kubernetes config: %s", err)
	}

	c, err := newKubeAPIClient(cfg)
	if err != nil {
		logger.Fatalf("cannot create Kubernetes client: %s", err)
	}

	currentNodeName, err := getCurrentNodeName(c, isLocal)
	if err != nil {
		logger.Fatalf("cannot get current node name: %s", err)
	}

	path := *checkpointsPath
	if len(path) == 0 {
		path = filepath.Join(tmpDataPath, "vlagent-kubernetes-checkpoints.json")
	}

	var excludeF *logstorage.Filter
	if *excludeFilter != "" {
		excludeF, err = logstorage.ParseFilter(*excludeFilter)
		if err != nil {
			logger.Fatalf("cannot parse LogsQL -kubernetesCollector.excludeFilter=%q: %s", *excludeFilter, err)
		}
	}

	kc, err := startKubernetesCollector(c, currentNodeName, *logsPath, path, excludeF)
	if err != nil {
		logger.Fatalf("cannot start kubernetes collector: %s", err)
	}
	collector = kc

	logger.Infof("started Kubernetes log collector for node %q", currentNodeName)
}

func Stop() {
	if collector != nil {
		collector.stop()
	}
}

func getCurrentNodeName(client *kubeAPIClient, isLocal bool) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if isLocal {
		return getCurrentNodeNameLocal(ctx, client)
	}
	return getCurrentNodeNameInCluster(ctx, client)
}

func isCSIIncluded(fields []logstorage.Field) bool {
	namespace := ""
	release := ""
	component := ""
	for _, field := range fields {
		switch field.Name {
		case "kubernetes.pod_namespace":
			namespace = field.Value
		case "kubernetes.pod_labels.release":
			release = field.Value
		case "kubernetes.pod_labels.component":
			component = field.Value
		}
	}
	if component != "nodeplugin" && component != "provisioner" {
		return false
	}
	switch namespace {
	case "rbd-hdd":
		return *csiRbdHdd && release == namespace
	case "rbd-ssd":
		return *csiRbdSsd && release == namespace
	case "rbd-nvme":
		return *csiRbdNvme && release == namespace
	default:
		return false
	}
}

func getCurrentNodeNameLocal(ctx context.Context, client *kubeAPIClient) (string, error) {
	nodes, err := client.getNodes(ctx)
	if err != nil {
		return "", fmt.Errorf("cannot get nodes from the cluster: %w", err)
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("cannot find any nodes in the cluster")
	}
	firstNode := nodes[0]
	return firstNode, nil
}

func getCurrentNodeNameInCluster(ctx context.Context, client *kubeAPIClient) (string, error) {
	ns, err := getCurrentNamespace()
	if err != nil {
		return "", fmt.Errorf("cannot get current namespace: %w", err)
	}

	podName, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("cannot get hostname: %w", err)
	}

	currentPod, err := client.getPod(ctx, ns, podName)
	if err != nil {
		return "", fmt.Errorf("cannot get pod %q at namespace %q: %w", podName, ns, err)
	}

	return currentPod.Spec.NodeName, nil
}

func getCurrentNamespace() (string, error) {
	ns, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", fmt.Errorf("cannot read current namespace: %w", err)
	}
	ns = bytes.TrimSpace(ns)
	return string(ns), nil
}

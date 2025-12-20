# Running vlagent outside Kubernetes for local development

This guide explains how to run vlagent outside a Kubernetes cluster
for local development and testing purposes.

## Prerequisites

Install [k3d](https://github.com/k3d-io/k3d) - a lightweight tool for running Kubernetes locally,
with support for mounting `/var/log/*` folders from the guest to the host system.

## Setup

1. Create `/var/log/containers` and `/var/log/pods` directories on your host system:

```bash
mkdir -p /var/log/containers /var/log/pods

# Ensure both vlagent and k3s have read/write permissions
chmod a+rw /var/log/pods /var/log/containers
```

2. Create a k3d cluster with proper volume mounts:

```bash
k3d cluster create -v /var/log/containers:/var/log/containers@all -v /var/log/pods:/var/log/pods@all
```

This command will also update the `~/.kube/config` file to use the new k3d cluster.
vlagent will use this kubeconfig file to connect to the currently selected cluster.
You can change the kubeconfig path via the `KUBECONFIG` environment variable.

3. Run vlagent with Kubernetes discovery enabled:

```bash
./vlagent -remoteWrite.url=http://localhost:9428/insert/native -kubernetesCollector
```

vlagent connects to the Kubernetes API to discover pods and containers running in the cluster.
It reads logs from the `/var/log/containers` and `/var/log/pods` directories mounted on the host system.

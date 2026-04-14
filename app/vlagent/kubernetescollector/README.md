# Running vlagent outside Kubernetes for local development

This guide explains how to run vlagent outside a Kubernetes cluster
for local development and testing purposes.

## Prerequisites

Install [k3d](https://github.com/k3d-io/k3d) - a lightweight tool for running Kubernetes locally,
with support for mounting `/var/log/*` folders from the guest to the host system.

## Setup

### Create Kubernetes logs directories with proper permissions

#### MacOS

```sh
sudo mkdir -p /var/log/pods /var/log/containers
# Grant read and write permissions on log folders to all users
sudo chmod a+rw /var/log/pods /var/log/containers
```

#### Linux

On macOS, files created inside mounted folders inherit the permissions of the parent folder,
so a simple `chmod` on the folder is enough.
On Linux, new files are always created by root.
We use `setfacl` with default ACL (`d:`) to define a rule that is automatically
applied to all new files created inside the folder.

```sh
sudo mkdir -p /var/log/pods /var/log/containers
sudo setfacl -R -m u:$(whoami):rx,d:u:$(whoami):rx /var/log/pods /var/log/containers
```

### Create a k3d cluster with proper volume mounts:

```sh
k3d cluster create test -v /var/log/containers:/var/log/containers@all -v /var/log/pods:/var/log/pods@all
```

This command will also update the `~/.kube/config` file to use the new k3d cluster.
vlagent will use this kubeconfig file to connect to the currently selected cluster.
You can change the kubeconfig path via the `KUBECONFIG` environment variable.

### Run vlagent with Kubernetes discovery enabled:

```sh
./vlagent -remoteWrite.url=http://localhost:9428/insert/native -kubernetesCollector
```

vlagent connects to the Kubernetes API to discover Pods and containers running in the cluster.
It reads logs from the `/var/log/containers` and `/var/log/pods` directories mounted on the host system.

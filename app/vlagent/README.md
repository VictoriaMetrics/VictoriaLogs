See vlagent docs [here](https://docs.victoriametrics.com/victorialogs/vlagent/).

The sources of vlagent docs are located [here](https://github.com/VictoriaMetrics/VictoriaLogs/blob/master/docs/victorialogs/vlagent.md).

Local file storage can be enabled together with remote write:

```text
-localStorage.enabled=true
-localStorage.logPath=/apps/cmss-vlagent-logs
-localStorage.fileName=vlagent.log
-localStorage.workerCount=4
-localStorage.maxSize=50
-localStorage.maxAge=7
-localStorage.maxBackups=10
-localStorage.localTime=true
-localStorage.compress=true
-localStorage.routeByKubernetes=true
-localStorage.virtLauncher.workerCount=16
-localStorage.virtLauncher.maxSize=1024
-localStorage.virtLauncher.maxAge=30
-localStorage.virtLauncher.maxBackups=60
-localStorage.virtLauncher.localTime=true
-localStorage.virtLauncher.compress=true
```

The local sink writes normalized log rows as JSON Lines. Known KubeVirt, CDI and enabled CSI logs are routed to component-specific files such as `kubevirt/virt-launcher.log`, `cdi/cdi-apiserver.log` and `csi/csi-rbd-hdd-nodeplugin.log`; other logs use `vlagent.log`. `virt-launcher` uses its own rotation and worker settings, matching cmss-loggie's `ClusterLogConfig`; other files use the common local storage settings. Active files are rotated by size, and rotated files are cleaned up by age and backup count. Remote write remains controlled by `-remoteWrite.url` and its persistent queue.

CSI collection is disabled by default. Enable the corresponding flags when the matching `cmss-loggie` `logConfigCSI` switch is enabled:

```text
-kubernetesCollector.csiRbdHdd=true
-kubernetesCollector.csiRbdSsd=true
-kubernetesCollector.csiRbdNvme=true
```

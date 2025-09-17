# Requirements

Dashboards require VictoriaLogs and vlagent at version 1.29.0 or later.

You can find the latest version of these dashboards on the [Grafana website](https://grafana.com/orgs/victoriametrics).

Use Prometheus datasource with these dashboards. For VictoriaMetrics integration, use the modified versions in the `vm/` folder which are configured for the VictoriaMetrics datasource. See more details about how to configure monitoring in the [VictoriaLogs monitoring documentation](https://docs.victoriametrics.com/victorialogs/#monitoring).

# Description

These dashboards contain comprehensive monitoring sections organized by functionality areas:

- **Stats** - High-level metrics including total log entries, ingestion rates, disk usage, and system version information
- **Overview** - Real-time visualization of log ingestion rates, request patterns, error rates, and performance trends
- **Resource usage** - CPU utilization, memory consumption, network activity, garbage collection metrics, and system pressure indicators
- **Troubleshooting** - Debugging panels for error tracking, configuration validation, and operational diagnostics
- **Slow Query Troubleshooting** - Query performance analysis, optimization metrics, and latency diagnostics

Additional specialized sections include:

- **Storage** operations with merge performance and indexing metrics
- **Ingestion** pipeline monitoring with flush operations and data flow rates
- **Querying** performance with request latencies and timeout tracking
- For cluster deployments, dedicated sections monitor individual components including **vlstorage**, **vlinsert**, and **vlselect** with component-specific metrics and health indicators.

If you have suggestions for improvements or discover any issues, please feel free to create an [issue](https://github.com/VictoriaMetrics/VictoriaLogs/issues) or submit feedback through the dashboard review system.

More information about VictoriaLogs can be found in the [official documentation](https://docs.victoriametrics.com/victorialogs/).

New releases and container images are available at the [VictoriaLogs releases page](https://github.com/VictoriaMetrics/VictoriaLogs/releases) and [DockerHub](https://hub.docker.com/u/victoriametrics?page=1&search=logs).

# Development

The `vm` folder contains copies of the main dashboards that have been modified to use the [VictoriaMetrics datasource](https://github.com/VictoriaMetrics/victoriametrics-datasource) instead of the standard Prometheus datasource. This allows for better integration when using VictoriaMetrics as the metrics storage backend.

All dashboards are available on the [Grafana website](https://grafana.com/orgs/victoriametrics/dashboards) and can be imported directly into your Grafana instance. When making changes to dashboards in the main `dashboards` folder, remember to run `make dashboards-sync` to update the VictoriaMetrics-compatible versions and sync any changes to the Grafana website.
dashboard-copy:
	echo "" > dashboards/vm/${SRC}
	cat dashboards/${SRC} >> dashboards/vm/${SRC}
	sed -i='.tmp' 's/prometheus/victoriametrics-metrics-datasource/g' dashboards/vm/${SRC}
	sed -i='.tmp' 's/Prometheus/VictoriaMetrics/g' dashboards/vm/${SRC}
	sed -i='.tmp' 's/${D_UID}/${D_UID}_vm/g' dashboards/vm/${SRC}
	sed -i='.tmp' 's/"title": "${TITLE}"/"title": "${TITLE} (VM)"/g' dashboards/vm/${SRC}
	rm -rf dashboards/vm/*.tmp

# Copies listed dashboards to vm/* but changes the datasource type from Prometheus to VictoriaMetrics.
# The command should be called before committing changes to dashboards/* files.
dashboards-sync:
	SRC=vlagent.json D_UID=Y5Z9GzMGz TITLE="VictoriaLogs - vlagent" $(MAKE) dashboard-copy
	SRC=victorialogs.json D_UID=OqPIZTX4z TITLE="VictoriaLogs - single-node" $(MAKE) dashboard-copy
	SRC=victorialogs-cluster.json D_UID=XqCOFEX4z TITLE="VictoriaLogs - cluster" $(MAKE) dashboard-copy

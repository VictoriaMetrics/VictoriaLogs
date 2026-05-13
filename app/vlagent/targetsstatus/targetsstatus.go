package targetsstatus

import (
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/filecollector"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlagent/kubernetescollector"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func RequestHandler(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return false
	}

	var data []targetsData

	debugInfo := kubernetescollector.DebugInfo()
	if len(debugInfo) > 0 {
		data = append(data, targetsData{
			Category: "Kubernetes Collector",
			Groups:   groupDebugInfo(debugInfo, "kubernetes.pod_namespace", "kubernetes.pod_name"),
		})
	}

	debugInfo = filecollector.DebugInfo()
	if len(debugInfo) > 0 {
		data = append(data, targetsData{
			Category: "File Collector",
			Groups:   groupDebugInfo(debugInfo, "glob_pattern", "file"),
		})
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	WriteTargetsPage(w, data)
	return true
}

type targetsData struct {
	// Category name e.g. Kubernetes Pods.
	Category string
	// Groups contains grouped entries by specific key, e.g. grouped by kubernetes.pod_namespace.
	Groups []targetGroup
}

type targetGroup struct {
	// Group name e.g. kube-system.
	Group string
	// Targets contains currently processing targets in the group, e.g. kube-proxy in a group kube-system.
	Targets []target
}

type target struct {
	// Preview of the target, e.g. Pod name.
	Preview string
	// DebugInfo contains all debug fields.
	DebugInfo []logstorage.Field
}

func groupDebugInfo(debugInfo [][]logstorage.Field, groupKey, titleKey string) []targetGroup {
	groupIndex := make(map[string]int)
	var groups []targetGroup
	for _, di := range debugInfo {
		groupName := mustFieldValue(di, groupKey)
		i, ok := groupIndex[groupName]

		// Create a new group.
		if !ok {
			i = len(groups)
			groupIndex[groupName] = i
			groups = append(groups, targetGroup{
				Group: groupName,
			})
		}

		groups[i].Targets = append(groups[i].Targets, target{
			Preview:   mustFieldValue(di, titleKey),
			DebugInfo: di,
		})
	}
	slices.SortFunc(groups, func(a, b targetGroup) int {
		return strings.Compare(a.Group, b.Group)
	})
	for _, group := range groups {
		slices.SortFunc(group.Targets, func(a, b target) int {
			return strings.Compare(a.Preview, b.Preview)
		})
	}
	return groups
}

func mustFieldValue(fields []logstorage.Field, key string) string {
	for _, f := range fields {
		if f.Name == key {
			return f.Value
		}
	}
	panic(fmt.Errorf("BUG: field %q not found", key))
}

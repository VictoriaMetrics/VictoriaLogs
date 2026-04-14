package logstorage

import (
	"fmt"
	"testing"
)

func BenchmarkCheckStreamFieldNames(b *testing.B) {
	fieldNames := []string{
		"collector",
		"kubernetes.container_name",
		"kubernetes.pod_name",
		"kubernetes.pod_namespace",
		"kubernetes.pod_node_name",
	}

	b.ReportAllocs()
	b.SetBytes(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := CheckStreamFieldNames(fieldNames); err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
		}
	})
}

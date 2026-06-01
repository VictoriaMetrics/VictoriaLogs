package logstorage

import (
	"testing"
	"time"
)

func BenchmarkTransforms(b *testing.B) {
	b.Run("set-tenant", func(b *testing.B) {
		program := `if (source:=Kubernetes) {
			format 1 as vl_account_id; format 2 as vl_project_id;
		} else if (source:=frontend) {
			format 3 as vl_account_id; format 4 as vl_project_id;
		} else if (source:=iOS) {
			format 5 as vl_account_id; format 6 as vl_project_id;
		} else if (source:=Android) {
			format 7 as vl_account_id; format 8 as vl_project_id;
		} else {
			format 0 as vl_account_id; format 0 as vl_project_id;
		}`
		content := []string{
			`{"_msg":"a message from Kubernetes", "source": "Kubernetes", "kubernetes.pod_labels.app":"VictoriaStack"}`,
			`{"_msg":"a message from front-end", "source": "fe", "page":"VMUI-web"}`,
			`{"_msg":"a message from iOS", "source": "iOS", "service":"VMUI-iOS"}`,
			`{"_msg":"a message from Android", "source": "Android", "app":"VMUI-Android"}`,
		}
		benchmarkTransforms(b, program, content)
	})

	b.Run("normalize-logfmt", func(b *testing.B) {
		program := `unpack_logfmt | rename msg as _msg, time as _time;`
		content := []string{
			`{"_msg":"level=error msg=\"context canceled\" db_host=127.0.0.1 retry_count=5 time=2026-06-10T11:02:02.000Z"}`,
			`{"_msg":"not a logfmt-encoded message"}`,
			`{"_msg":"msg=\"new request\" method=GET path=/login host=example.com status=200 duration=12ms"}`,
		}
		benchmarkTransforms(b, program, content)
	})

	b.Run("normalize-json", func(b *testing.B) {
		program := `unpack_json from payload | rename msg as _msg, time as _time | delete payload;`
		content := []string{
			`{"payload":"{\"level\":\"error\",\"msg\":\"context canceled\",\"db_host\":\"127.0.0.1\",\"retry_count\":5,\"time\":\"2026-06-10T11:02:02.000Z\"}"}`,
			`{"payload":"not a json-encoded message"}`,
			`{"payload":"{\"msg\":\"new request\",\"method\":\"GET\",\"path\":\"/login\",\"host\":\"example.com\",\"status\":200,\"duration\":\"12ms\"}"}`,
		}
		benchmarkTransforms(b, program, content)
	})

	b.Run("simplify-k8s-labels", func(b *testing.B) {
		program := `rename kubernetes.pod_labels.* as pod.*, kubernetes.pod_annotations.* as pod.*, kubernetes.* as k8s.*, k8s.pod_ip as ip, k8s.pod_name as pod, k8s.pod_node_name as node;`
		content := []string{
			`{
				"_msg": "Order confirmation email sent to: reed@example.com",
				"collector": "vlagent",
				"kubernetes.container_id": "containerd://da795e859cba3854e0f5e31ea5ae17279380499fdea05fc34ebc847bce4e31a5",
				"kubernetes.container_name": "email",
				"kubernetes.pod_ip": "10.71.1.150",
				"kubernetes.pod_labels.app.kubernetes.io/component": "email",
				"kubernetes.pod_labels.app.kubernetes.io/name": "email",
				"kubernetes.pod_labels.opentelemetry.io/name": "email",
				"kubernetes.pod_labels.pod-template-hash": "686bdfd59b",
				"kubernetes.pod_labels.topology.kubernetes.io/region": "us-east1",
				"kubernetes.pod_labels.topology.kubernetes.io/zone": "us-east1-b",
				"kubernetes.pod_name": "email-686bdfd59b-dd79g",
				"kubernetes.pod_namespace": "play-otel",
				"kubernetes.pod_node_name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-v88y"
			}`,
			`{
				"_msg": "Order confirmation email sent to: jack@example.com",
				"collector": "vector",
				"file": "/var/log/pods/play-otel_email-686bdfd59b-dd79g_745f66b9-2e21-4ee9-a053-a7cc849a0083/email/0.log",
				"kubernetes.container_id": "containerd://da795e859cba3854e0f5e31ea5ae17279380499fdea05fc34ebc847bce4e31a5",
				"kubernetes.container_image": "ghcr.io/open-telemetry/demo:2.1.3-email",
				"kubernetes.container_image_id": "ghcr.io/open-telemetry/demo@sha256:5a62bbc4c7f34292c37b7b04b5a85bb74fc7a50c99b0429bea684602472c211d",
				"kubernetes.container_name": "email",
				"kubernetes.namespace_labels.kubernetes.io/metadata.name": "play-otel",
				"kubernetes.node_labels.beta.kubernetes.io/arch": "amd64",
				"kubernetes.node_labels.beta.kubernetes.io/instance-type": "n2d-standard-8",
				"kubernetes.node_labels.beta.kubernetes.io/os": "linux",
				"kubernetes.node_labels.cloud.google.com/gke-boot-disk": "pd-balanced",
				"kubernetes.node_labels.cloud.google.com/gke-container-runtime": "containerd",
				"kubernetes.node_labels.cloud.google.com/gke-cpu-scaling-level": "8",
				"kubernetes.node_labels.cloud.google.com/gke-logging-variant": "DEFAULT",
				"kubernetes.node_labels.cloud.google.com/gke-max-pods-per-node": "110",
				"kubernetes.node_labels.cloud.google.com/gke-memory-gb-scaling-level": "32",
				"kubernetes.node_labels.cloud.google.com/gke-netd-ready": "true",
				"kubernetes.node_labels.cloud.google.com/gke-nodepool": "n2d-std-8-20260330102605160300000001",
				"kubernetes.node_labels.cloud.google.com/gke-os-distribution": "cos",
				"kubernetes.node_labels.cloud.google.com/gke-provisioning": "standard",
				"kubernetes.node_labels.cloud.google.com/gke-stack-type": "IPV4",
				"kubernetes.node_labels.cloud.google.com/machine-family": "n2d",
				"kubernetes.node_labels.disk-type.gke.io/hyperdisk-throughput": "true",
				"kubernetes.node_labels.disk-type.gke.io/pd-balanced": "true",
				"kubernetes.node_labels.disk-type.gke.io/pd-extreme": "true",
				"kubernetes.node_labels.disk-type.gke.io/pd-ssd": "true",
				"kubernetes.node_labels.disk-type.gke.io/pd-standard": "true",
				"kubernetes.node_labels.failure-domain.beta.kubernetes.io/region": "us-east1",
				"kubernetes.node_labels.failure-domain.beta.kubernetes.io/zone": "us-east1-b",
				"kubernetes.node_labels.iam.gke.io/gke-metadata-server-enabled": "true",
				"kubernetes.node_labels.kubernetes.io/arch": "amd64",
				"kubernetes.node_labels.kubernetes.io/hostname": "gke-sandbox-n2d-std-8-202603301026051-852214e6-v88y",
				"kubernetes.node_labels.kubernetes.io/os": "linux",
				"kubernetes.node_labels.node.kubernetes.io/instance-type": "n2d-standard-8",
				"kubernetes.node_labels.topology.gke.io/zone": "us-east1-b",
				"kubernetes.node_labels.topology.kubernetes.io/region": "us-east1",
				"kubernetes.node_labels.topology.kubernetes.io/zone": "us-east1-b",
				"kubernetes.pod_ip": "10.71.1.150",
				"kubernetes.pod_ips": "[\"10.71.1.150\"]",
				"kubernetes.pod_labels.app.kubernetes.io/component": "email",
				"kubernetes.pod_labels.app.kubernetes.io/name": "email",
				"kubernetes.pod_labels.opentelemetry.io/name": "email",
				"kubernetes.pod_labels.pod-template-hash": "686bdfd59b",
				"kubernetes.pod_labels.topology.kubernetes.io/region": "us-east1",
				"kubernetes.pod_labels.topology.kubernetes.io/zone": "us-east1-b",
				"kubernetes.pod_name": "email-686bdfd59b-dd79g",
				"kubernetes.pod_namespace": "play-otel",
				"kubernetes.pod_node_name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-v88y",
				"kubernetes.pod_owner": "ReplicaSet/email-686bdfd59b",
				"kubernetes.pod_uid": "745f66b9-2e21-4ee9-a053-a7cc849a0083",
				"source_type": "kubernetes_logs",
				"stream": "stdout"
			}`,
			`{
				"_msg": "Product Found",
				"app.product.id": "L9ECAV7KIM",
				"app.product.name": "Lens Cleaning Kit",
				"collector": "otel-collector",
				"host.name": "otel-collector-7f8479fcb7-s7kc2",
				"k8s.deployment.name": "product-catalog",
				"k8s.namespace.name": "play-otel",
				"k8s.node.name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-fx1b",
				"k8s.pod.ip": "10.71.10.33",
				"k8s.pod.name": "product-catalog-759d5f4b46-f6zfg",
				"k8s.pod.start_time": "2026-06-05T10:07:49Z",
				"k8s.pod.uid": "97463b02-e9d7-4a43-b164-e21c837e7b48",
				"os.type": "linux",
				"scope.name": "product-catalog",
				"scope.version": "unknown",
				"service.instance.id": "97463b02-e9d7-4a43-b164-e21c837e7b48",
				"service.name": "product-catalog",
				"service.namespace": "opentelemetry-demo",
				"service.version": "2.1.3",
				"severity_number": "9",
				"severity_text": "INFO",
				"span_id": "200124c35ba77ae1",
				"telemetry.sdk.language": "go",
				"telemetry.sdk.name": "opentelemetry",
				"telemetry.sdk.version": "1.38.0",
				"trace_id": "c56f09f96d570b55ae1890123ecbe4ce"
			}`,
		}
		benchmarkTransforms(b, program, content)
	})
}

func benchmarkTransforms(b *testing.B, program string, rows []string) {
	b.Helper()
	prog, err := ParseTransformsProgram(program)
	if err != nil {
		b.Fatal(err)
	}
	transformer := prog.NewTransformer(func(_ *LogRows) {
	})

	p := GetJSONParser()
	defer PutJSONParser(p)
	lr := GetLogRows(nil, nil, nil, nil, "missing _msg field in a bench")
	defer PutLogRows(lr)

	rowsSize := 0
	for _, row := range rows {
		rowsSize += len(row)
		if err := p.ParseLogMessage([]byte(row), nil, ""); err != nil {
			b.Fatal(err)
		}
		lr.MustAdd(TenantID{}, time.Now().UnixNano(), p.Fields, -1)
	}

	b.ReportAllocs()
	b.SetBytes(int64(rowsSize))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			transformer.Transform(lr)
		}
	})
}

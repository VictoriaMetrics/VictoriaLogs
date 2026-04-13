package logstorage

import (
	"fmt"
	"testing"
)

func BenchmarkLogRowsMustAdd(b *testing.B) {
	rows := newBenchRows(map[string]string{
		"input.type":         "filestream",
		"ecs.version":        "8.0.0",
		"host.hostname":      "foobar-baz-abc",
		"host.architecture":  "x86_64",
		"host.name":          "foobar-baz-abc",
		"host.os.codename":   "bionic",
		"host.os.type":       "linux",
		"host.os.platform":   "ubuntu",
		"host.os.version":    "18.04.6 LTS (Bionic Beaver)",
		"host.os.family":     "debian",
		"host.os.name":       "Ubuntu",
		"host.os.kernel":     "4.15.0-211-generic",
		"host.id":            "a634d50249af449dbcb3ce724822568a",
		"host.containerized": "false",
		"host.ip":            `["10.0.0.42","10.224.112.1","172.20.0.1","172.18.0.1","172.19.0.1","fc00:f853:ccd:e793::1","fe80::1","172.21.0.1","172.17.0.1"]`,
		"host.mac":           `["02-42-42-90-52-D9","02-42-C6-48-A6-84","02-42-FD-91-7E-17","52-54-00-F5-13-E7","54-E1-AD-89-1A-4C","F8-34-41-3C-C0-85"]`,
		"agent.ephemeral_id": "6c251f67-7210-4cef-8f72-a9546cbb48cc",
		"agent.id":           "e97243c5-5ef3-4dc1-8828-504f68731e87",
		"agent.name":         "foobar-baz-abc",
		"agent.type":         "filebeat",
		"agent.version":      "8.8.0",
		"log.file.path":      "/var/log/auth.log",
		"log.offset":         "37908",
	}, []string{
		"Jun  4 20:34:07 foobar-baz-abc sudo: pam_unix(sudo:session): session opened for user root by (uid=0)",
		"Jun  4 20:34:07 foobar-baz-abc sudo: pam_unix(sudo:session): session opened for user root by (uid=1)",
		"Jun  4 20:34:07 foobar-baz-abc sudo: pam_unix(sudo:session): session opened for user root by (uid=2)",
		"Jun  4 20:34:07 foobar-baz-abc sudo: pam_unix(sudo:session): session opened for user root by (uid=3)",
		"Jun  4 20:34:07 foobar-baz-abc sudo: pam_unix(sudo:session): session opened for user root by (uid=4)",
	})
	streamFields := []string{
		"host.hostname",
		"agent.name",
		"log.file.path",
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(rows)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			benchmarkLogRowsMustAdd(rows, streamFields)
		}
	})
}

func benchmarkLogRowsMustAdd(rows [][]Field, streamFields []string) {
	lr := GetLogRows(streamFields, nil, nil, nil, "")
	var tid TenantID
	for i, fields := range rows {
		tid.AccountID = uint32(i)
		tid.ProjectID = uint32(2 * i)
		timestamp := int64(i) * 1000
		lr.mustAdd(tid, timestamp, fields)
	}
	PutLogRows(lr)
}

func newBenchRows(constFields map[string]string, messages []string) [][]Field {
	rows := make([][]Field, 0, len(messages))
	for _, msg := range messages {
		row := make([]Field, 0, len(constFields)+1)
		for k, v := range constFields {
			row = append(row, Field{
				Name:  k,
				Value: v,
			})
		}
		row = append(row, Field{
			Name:  "_msg",
			Value: msg,
		})
		rows = append(rows, row)
	}
	return rows
}

func BenchmarkVerifyStreamTagsCanonical(b *testing.B) {
	type case_ struct {
		streamTags string
		fields     string
	}
	cases := []case_{
		{
			streamTags: `{collector="otel-collector",k8s.namespace.name="play-otel",k8s.pod.name="product-catalog-865cbcbfd5-7zpgl",service.name="product-catalog"}`,
			fields: `{
    "_msg": "Product Found",
    "_time": "2026-04-11T08:38:14.974052579Z",
    "app.product.id": "66VCHSJNUP",
    "app.product.name": "Starsense Explorer Refractor Telescope",
    "collector": "otel-collector",
    "host.name": "otel-collector-b9dd8f965-4h8zs",
    "k8s.deployment.name": "product-catalog",
    "k8s.namespace.name": "play-otel",
    "k8s.node.name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-6vfd",
    "k8s.pod.ip": "10.71.12.36",
    "k8s.pod.name": "product-catalog-865cbcbfd5-7zpgl",
    "k8s.pod.start_time": "2026-03-30T10:51:34Z",
    "k8s.pod.uid": "a7bac2db-9d25-4ac4-8186-e7f701db0297",
    "os.type": "linux",
    "scope.name": "product-catalog",
    "scope.version": "unknown",
    "service.instance.id": "a7bac2db-9d25-4ac4-8186-e7f701db0297",
    "service.name": "product-catalog",
    "service.namespace": "opentelemetry-demo",
    "service.version": "2.1.3",
    "severity": "INFO",
    "span_id": "3348790edf0b0efb",
    "telemetry.sdk.language": "go",
    "telemetry.sdk.name": "opentelemetry",
    "telemetry.sdk.version": "1.38.0",
    "trace_id": "859977c5146616a3c06bd699eb2f9f99"}`,
		},
		{
			streamTags: `{collector="vector",kubernetes.container_name="cart",kubernetes.pod_name="cart-b664bfdf7-dxgt7",kubernetes.pod_namespace="play-otel"}`,
			fields: `{
    "_msg": "      GetCartAsync called with userId=",
    "_time": "2026-04-11T08:38:14.43448393Z",
    "collector": "vector",
    "file": "/var/log/pods/play-otel_cart-b664bfdf7-dxgt7_db60db4e-01d4-450b-bd9c-9b3cc976d63e/cart/0.log",
    "kubernetes.container_id": "containerd://ece58acd3639c2699975eb947a2ca667d6d9b1a7efe2d832718f87d64353b6a0",
    "kubernetes.container_image": "ghcr.io/open-telemetry/demo:2.1.3-cart",
    "kubernetes.container_image_id": "ghcr.io/open-telemetry/demo@sha256:84ce75e56697c94a8ab00258b68babc06c042ae6f1e98c061f52f0c748d0cdd5",
    "kubernetes.container_name": "cart",
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
    "kubernetes.node_labels.kubernetes.io/hostname": "gke-sandbox-n2d-std-8-202603301026051-852214e6-6vfd",
    "kubernetes.node_labels.kubernetes.io/os": "linux",
    "kubernetes.node_labels.node.kubernetes.io/instance-type": "n2d-standard-8",
    "kubernetes.node_labels.topology.gke.io/zone": "us-east1-b",
    "kubernetes.node_labels.topology.kubernetes.io/region": "us-east1",
    "kubernetes.node_labels.topology.kubernetes.io/zone": "us-east1-b",
    "kubernetes.pod_ip": "10.71.12.38",
    "kubernetes.pod_ips": "[\"10.71.12.38\"]",
    "kubernetes.pod_labels.app.kubernetes.io/component": "cart",
    "kubernetes.pod_labels.app.kubernetes.io/name": "cart",
    "kubernetes.pod_labels.opentelemetry.io/name": "cart",
    "kubernetes.pod_labels.pod-template-hash": "b664bfdf7",
    "kubernetes.pod_labels.topology.kubernetes.io/region": "us-east1",
    "kubernetes.pod_labels.topology.kubernetes.io/zone": "us-east1-b",
    "kubernetes.pod_name": "cart-b664bfdf7-dxgt7",
    "kubernetes.pod_namespace": "play-otel",
    "kubernetes.pod_node_name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-6vfd",
    "kubernetes.pod_owner": "ReplicaSet/cart-b664bfdf7",
    "kubernetes.pod_uid": "db60db4e-01d4-450b-bd9c-9b3cc976d63e",
    "source_type": "kubernetes_logs",
    "stream": "stdout"}`,
		},
		{
			streamTags: `{collector="vector",kubernetes.container_name="ad",kubernetes.pod_name="ad-75947945cc-2dt89",kubernetes.pod_namespace="play-otel"}`,
			fields: `{
    "_msg": "2026-04-11 08:38:12 - oteldemo.AdService - Targeted ad request received for [telescopes] trace_id=14ef1b0a5d60477d9cd9d22430510e7f span_id=0326713c147fb495 trace_flags=01 ",
    "_time": "2026-04-11T08:38:12.648632385Z",
    "collector": "vector",
    "file": "/var/log/pods/play-otel_ad-75947945cc-2dt89_90405091-7da3-4759-8884-9b68dc6caae5/ad/0.log",
    "kubernetes.container_id": "containerd://b1f6223504aceb8c96446d7c56eb7271192fb247e2990fef2208f5e27e2f7fe4",
    "kubernetes.container_image": "ghcr.io/open-telemetry/demo:2.1.3-ad",
    "kubernetes.container_image_id": "ghcr.io/open-telemetry/demo@sha256:c9d4f94314937eb3b61cf8b1672b7e87fab95442bd1d01f8d32f43a882255944",
    "kubernetes.container_name": "ad",
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
    "kubernetes.node_labels.kubernetes.io/hostname": "gke-sandbox-n2d-std-8-202603301026051-852214e6-6vfd",
    "kubernetes.node_labels.kubernetes.io/os": "linux",
    "kubernetes.node_labels.node.kubernetes.io/instance-type": "n2d-standard-8",
    "kubernetes.node_labels.topology.gke.io/zone": "us-east1-b",
    "kubernetes.node_labels.topology.kubernetes.io/region": "us-east1",
    "kubernetes.node_labels.topology.kubernetes.io/zone": "us-east1-b",
    "kubernetes.pod_ip": "10.71.12.33",
    "kubernetes.pod_ips": "[\"10.71.12.33\"]",
    "kubernetes.pod_labels.app.kubernetes.io/component": "ad",
    "kubernetes.pod_labels.app.kubernetes.io/name": "ad",
    "kubernetes.pod_labels.opentelemetry.io/name": "ad",
    "kubernetes.pod_labels.pod-template-hash": "75947945cc",
    "kubernetes.pod_labels.topology.kubernetes.io/region": "us-east1",
    "kubernetes.pod_labels.topology.kubernetes.io/zone": "us-east1-b",
    "kubernetes.pod_name": "ad-75947945cc-2dt89",
    "kubernetes.pod_namespace": "play-otel",
    "kubernetes.pod_node_name": "gke-sandbox-n2d-std-8-202603301026051-852214e6-6vfd",
    "kubernetes.pod_owner": "ReplicaSet/ad-75947945cc",
    "kubernetes.pod_uid": "90405091-7da3-4759-8884-9b68dc6caae5",
    "source_type": "kubernetes_logs",
    "stream": "stdout"}`,
		},
	}

	type caseReal struct {
		streamTagsCanonical string
		fields              []Field
	}
	casesReal := make([]caseReal, len(cases))
	for i, c := range cases {
		casesReal[i] = caseReal{
			streamTagsCanonical: toStreamTagsCanonical(c.streamTags),
			fields:              toFields(c.fields),
		}
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(casesReal)))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, c := range casesReal {
				err := verifyStreamTagsCanonical(c.streamTagsCanonical, c.fields)
				if err != nil {
					panic(fmt.Errorf("unexpected error: %s", err))
				}
			}
		}
	})
}

func toStreamTagsCanonical(streamTags string) string {
	st := GetStreamTags()
	defer PutStreamTags(st)
	if err := st.unmarshalStringInplace(streamTags); err != nil {
		panic(fmt.Errorf("cannot unmarshal streamTags %s: %w", streamTags, err))
	}
	return string(st.MarshalCanonical(nil))
}

func toFields(fieldsStr string) []Field {
	var p JSONParser
	if err := p.ParseLogMessage([]byte(fieldsStr), nil, ""); err != nil {
		panic(fmt.Errorf("cannot unmarshal fieldsStr %s: %w", fieldsStr, err))
	}
	return p.Fields
}

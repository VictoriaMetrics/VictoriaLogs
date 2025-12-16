module github.com/VictoriaMetrics/VictoriaLogs

go 1.25.5

replace github.com/VictoriaMetrics/VictoriaMetrics => github.com/VictoriaMetrics/VictoriaMetrics v1.132.1-0.20251216144623-b6bc1860136e

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.132.0
	github.com/VictoriaMetrics/easyproto v1.1.3
	github.com/VictoriaMetrics/metrics v1.40.2
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/ergochat/readline v0.1.3
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.18.2
	github.com/mattn/go-isatty v0.0.20
	github.com/valyala/fastjson v1.6.7
	github.com/valyala/fastrand v1.1.0
	github.com/valyala/quicktemplate v1.8.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/VictoriaMetrics/metricsql v0.84.8 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/gozstd v1.24.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	golang.org/x/oauth2 v0.34.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)

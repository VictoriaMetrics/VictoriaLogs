module github.com/VictoriaMetrics/VictoriaLogs

go 1.26.5

replace github.com/VictoriaMetrics/VictoriaMetrics => ../VictoriaMetrics

require (
	github.com/VictoriaMetrics/VictoriaMetrics v1.146.1-0.20260630165203-c82127b6d4d1
	github.com/VictoriaMetrics/easyproto v1.2.0
	github.com/VictoriaMetrics/metrics v1.44.0
	github.com/bmatcuk/doublestar/v4 v4.10.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/ergochat/readline v0.1.3
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/klauspost/compress v1.19.1
	github.com/mattn/go-isatty v0.0.23
	github.com/valyala/fastjson v1.6.10
	github.com/valyala/fastrand v1.1.0
	github.com/valyala/quicktemplate v1.8.0
	gopkg.in/yaml.v2 v2.4.0
)

require golang.org/x/sync v0.22.0 // indirect

require (
	github.com/VictoriaMetrics/metricsql v0.87.3 // indirect
	github.com/aws/aws-sdk-go-v2 v1.43.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.14 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.31 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.30 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.31 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager v0.3.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.31 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.32 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.32 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.106.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.33.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.38.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.0 // indirect
	github.com/aws/smithy-go v1.27.4 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.2 // indirect
	github.com/valyala/gozstd v1.25.0 // indirect
	github.com/valyala/histogram v1.2.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

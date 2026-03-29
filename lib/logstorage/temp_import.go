package logstorage

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var Manager *transfermanager.Client
var NoSuchBucket *types.NoSuchBucket
var ChecksumCalculation aws.RequestChecksumCalculationWhenRequired
var AWSRetryMaxAttempts = retry.DefaultMaxAttempts
var AWSHTTPIdleConns  = http.DefaultHTTPTransportMaxIdleConns
var AWSLoadOpts *config.LoadOptions
var AWSS3Opts s3.Options

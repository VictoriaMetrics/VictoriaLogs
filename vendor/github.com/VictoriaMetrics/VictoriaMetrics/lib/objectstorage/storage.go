package objectstorage

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/common"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/objectstorage/s3"
)

type StorageConfig struct {
	// Destination is location to which data is loaded after offload period.
	// Example: s3://bucket-name/path/to/dir
	Destination string
	Cache       CachedStorageConfig
	S3          s3.StorageConfig
}

// New returns new storage backend
func New(cfg StorageConfig, stopCh <-chan struct{}) common.StorageClient {
	if len(cfg.Destination) == 0 {
		logger.Panicf("path is required for storage")
	}
	n := strings.Index(cfg.Destination, "://")
	if n < 0 {
		logger.Panicf("storage path should be prefixed with <proto>://")
	}
	var sc common.StorageClient
	scheme := cfg.Destination[:n]
	dir := cfg.Destination[n+len("://"):]
	switch scheme {
	case "s3":
		var bucket, prefix string
		if n := strings.Index(dir, "/"); n < 0 {
			bucket = dir
		} else {
			bucket = dir[:n]
			prefix = dir[n+1:]
		}
		if bucket == "" {
			logger.Panicf("bucket name is missing in storage path %q", cfg.Destination)
		}
		s, err := s3.New(bucket, prefix, cfg.S3, stopCh)
		if err != nil {
			logger.Panicf("cannot initialize connection to s3: %s", err)
		}
		sc = s
	default:
		logger.Panicf("unsupported scheme of path=%s, supported schemes: `s3://`", cfg.Destination)
	}
	return newCachedStorageClient(sc, cfg.Cache, stopCh)
}

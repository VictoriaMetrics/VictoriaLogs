package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/cgroup"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/contextutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/netutil"
	"github.com/VictoriaMetrics/metrics"
)

type StorageConfig struct {
	CredsFile          string
	ConfigFile         string
	ProfileName        string
	ForcePathStyle     bool
	Endpoint           string
	Region             string
	InsecureSkipVerify bool
	CompatMode         bool
}

var (
	concurrency = cgroup.AvailableCPUs()

	writtenBytesTotal = metrics.NewCounter(`vm_objectstorage_s3_written_bytes_total`)
	readBytesTotal    = metrics.NewCounter(`vm_objectstorage_s3_read_bytes_total`)
)

const maxDeleteObjects = 1_000

type StorageClient struct {
	ctx        context.Context
	cancel     func()
	bucket     string
	prefix     string
	client     *s3.Client
	manager    *transfermanager.Client
	compatMode bool
}

func New(bucket, prefix string, cfg StorageConfig, stopCh <-chan struct{}) (*StorageClient, error) {
	ctx, cancel := contextutil.NewStopChanContext(stopCh)
	configOpts := []func(*config.LoadOptions) error{
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		config.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = retry.NewExponentialJitterBackoff(5 * time.Second)
				o.MaxAttempts = 3
				o.Retryables = append(retry.DefaultRetryables, retry.RetryableErrorCode{
					Codes: map[string]struct{}{
						"IncompleteBody": {},
						"ExpiredToken":   {},
					},
				})
			})
		}),
	}
	if len(cfg.Region) > 0 {
		configOpts = append(configOpts, config.WithRegion(cfg.Region))
	}
	if len(cfg.ProfileName) > 0 {
		configOpts = append(configOpts, config.WithSharedConfigProfile(cfg.ProfileName))
	}
	if len(cfg.ConfigFile) > 0 {
		configOpts = append(configOpts, config.WithSharedConfigFiles([]string{
			cfg.ConfigFile,
		}))
	}
	if len(cfg.CredsFile) > 0 {
		configOpts = append(configOpts, config.WithSharedCredentialsFiles([]string{
			cfg.CredsFile,
		}))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx,
		configOpts...,
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cannot load S3 config: %w", err)
	}

	c := awshttp.NewBuildableClient()
	if awsCfg.HTTPClient != nil {
		trOpts, ok := awsCfg.HTTPClient.(*awshttp.BuildableClient)
		if ok {
			c = trOpts
		}
	}
	awsCfg.HTTPClient = c.WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = 100
		t.IdleConnTimeout = 90 * time.Second
		t.ResponseHeaderTimeout = 20 * time.Second
		if cfg.InsecureSkipVerify {
			if t.TLSClientConfig == nil {
				t.TLSClientConfig = &tls.Config{}
			}
			t.TLSClientConfig.InsecureSkipVerify = true
		}
		t.DialContext = netutil.NewStatDialFunc("objectstorage_s3_client")
	})
	var outerErr error
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if len(cfg.Endpoint) > 0 {
			if _, err := url.Parse(cfg.Endpoint); err != nil {
				outerErr = fmt.Errorf("cannot parse custom S3 endpoint: %s", cfg.Endpoint)
				return
			}
			logger.Infof("Using provided custom S3 endpoint: %s", cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
			o.BaseEndpoint = &cfg.Endpoint
		}
		if cfg.CompatMode {
			o.APIOptions = append(o.APIOptions, stripSDKTrackingHeaders)
		}
	})
	if outerErr != nil {
		cancel()
		return nil, outerErr
	}
	manager := transfermanager.New(client, func(o *transfermanager.Options) {
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	s := &StorageClient{
		client:     client,
		manager:    manager,
		bucket:     bucket,
		prefix:     prefix,
		ctx:        ctx,
		cancel:     cancel,
		compatMode: cfg.CompatMode,
	}
	return s, nil
}

func stripSDKTrackingHeaders(stack *smithymiddleware.Stack) error {
	_, _ = stack.Build.Remove((*awsmiddleware.ClientRequestID)(nil).ID())
	_, _ = stack.Finalize.Remove((*retry.MetricsHeader)(nil).ID())
	_, _ = stack.Finalize.Remove("DisableAcceptEncodingGzip")
	return nil
}

func singleFileUploadConcurrency(o *transfermanager.Options) {
	o.Concurrency = 1
}

func withContentMD5Checksum(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *smithymiddleware.Stack) error {
		_, _ = stack.Initialize.Remove("AWSChecksum:SetupInputContext")
		_, _ = stack.Build.Remove("AWSChecksum:RequestMetricsTracking")
		_, _ = stack.Finalize.Remove("AWSChecksum:ComputeInputPayloadChecksum")
		_, _ = stack.Finalize.Remove("addInputChecksumTrailer")
		return smithyhttp.AddContentChecksumMiddleware(stack)
	})
}

func (sc *StorageClient) Close() {
	sc.cancel()
}

func (sc *StorageClient) CreateFile(path string, data []byte) error {
	_, err := sc.manager.UploadObject(sc.ctx, &transfermanager.UploadObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(sc.getPath(path)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("failed to upload to %s: %w", sc.GetPath(path), err)
	}
	writtenBytesTotal.Add(len(data))
	return nil
}

func (sc *StorageClient) ListFiles(path string) (map[string]uint64, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(sc.bucket),
		Prefix: aws.String(sc.getPrefix(path)),
	}
	files := make(map[string]uint64)
	paginator := s3.NewListObjectsV2Paginator(sc.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(sc.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page of objects: %w", err)
		}
		sc.objectsToFiles(files, path, page.Contents)
	}
	return files, nil
}

func (sc *StorageClient) ReadDir(path string) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(sc.bucket),
		Prefix:    aws.String(sc.getPrefix(path)),
		Delimiter: aws.String("/"),
	}
	paginator := s3.NewListObjectsV2Paginator(sc.client, input)
	var dirs []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(sc.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get next page of objects: %w", err)
		}
		dirs = sc.objectsToDirs(dirs, path, page.CommonPrefixes)
	}
	return dirs, nil
}

func (sc *StorageClient) GetRoot() string {
	if sc.prefix == "" {
		return fmt.Sprintf("s3://%s/", sc.bucket)
	}
	return fmt.Sprintf("s3://%s/%s/", sc.bucket, sc.prefix)
}

func (sc *StorageClient) ReadRange(path string, buf []byte, offset int64) (int, error) {
	result, err := sc.client.GetObject(sc.ctx, &s3.GetObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(sc.getPath(path)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+int64(len(buf)-1))),
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get object %s: %w", sc.GetPath(path), err)
	}
	defer result.Body.Close()
	var n int
	if result.ContentLength == nil {
		n, err = io.ReadFull(result.Body, buf)
	} else {
		sz := min(int(*result.ContentLength), len(buf))
		n, err = io.ReadFull(result.Body, buf[:sz])
	}
	if err == nil {
		readBytesTotal.Add(n)
	}
	return n, err
}

func (sc *StorageClient) ReadFile(path string, wr io.WriterAt) error {
	out, err := sc.manager.DownloadObject(sc.ctx, &transfermanager.DownloadObjectInput{
		Bucket:   aws.String(sc.bucket),
		Key:      aws.String(sc.getPath(path)),
		WriterAt: wr,
	})
	if err != nil {
		return fmt.Errorf("failed to get object %s: %w", sc.GetPath(path), err)
	}
	if out.ContentLength != nil {
		readBytesTotal.Add(int(*out.ContentLength))
	}
	return nil
}

func (sc *StorageClient) GetFileSize(p string) (uint64, error) {
	o, err := sc.headObject(p)
	if err != nil {
		if isNotFoundErr(err) {
			return 0, fmt.Errorf("%s: %w", sc.GetPath(p), os.ErrNotExist)
		}
		return 0, err
	}
	if o.ContentLength == nil {
		return 0, nil
	}
	return uint64(*o.ContentLength), nil
}

func (sc *StorageClient) GetPath(p string) string {
	path := filepath.Join(sc.bucket, sc.prefix, p)
	if filepath.Separator != '/' {
		path = strings.ReplaceAll(path, string(filepath.Separator), "/")
	}
	return "s3://" + path
}

func (sc *StorageClient) getPath(p string) string {
	path := filepath.Join(sc.prefix, p)
	if filepath.Separator != '/' {
		path = strings.ReplaceAll(path, string(filepath.Separator), "/")
	}
	return path
}

func (sc *StorageClient) getPrefix(p string) string {
	return sc.getPath(p) + "/"
}

func (sc *StorageClient) IsFileExist(p string) (bool, error) {
	_, err := sc.headObject(p)
	if err != nil {
		if isNotFoundErr(err) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func isNotFoundErr(err error) bool {
	var nf *types.NotFound
	return errors.As(err, &nf)
}

func (_ *StorageClient) IsTransientError(e error) bool {
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return false
	}
	var noSuchBucket *types.NoSuchBucket
	var noSuchKey *types.NoSuchKey
	var notFound *types.NotFound
	if errors.As(e, &noSuchBucket) || errors.As(e, &noSuchKey) || errors.As(e, &notFound) {
		return false
	}
	var respErr *awshttp.ResponseError
	if errors.As(e, &respErr) {
		code := respErr.HTTPStatusCode()
		return code == http.StatusTooManyRequests || code >= http.StatusInternalServerError
	}
	return true
}

func (sc *StorageClient) DeletePath(path string, recursive bool) error {
	if !recursive {
		_, err := sc.client.DeleteObject(sc.ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(sc.bucket),
			Key:    aws.String(sc.getPath(path)),
		})
		return err
	}

	filesToDelete, err := sc.ListFiles(path)
	if err != nil {
		return fmt.Errorf("failed to list files at %s that are expected to be removed: %w", path, err)
	}

	var matchedFiles []types.ObjectIdentifier
	for fileToDelete := range filesToDelete {
		matchedFiles = append(matchedFiles, types.ObjectIdentifier{
			Key: aws.String(sc.getPath(filepath.Join(path, fileToDelete))),
		})
	}

	var wg sync.WaitGroup
	var start int
	var resultErr error
	var once sync.Once
	concurrencyChan := make(chan struct{}, concurrency)
	ctx, cancel := context.WithCancel(sc.ctx)
	defer cancel()
	for end := maxDeleteObjects; start < len(matchedFiles); end += maxDeleteObjects {
		end = min(end, len(matchedFiles))
		concurrencyChan <- struct{}{}
		batch := matchedFiles[start:end]
		wg.Go(func() {
			defer func() { <-concurrencyChan }()
			if err := sc.deleteObjects(ctx, batch); err != nil {
				once.Do(func() {
					resultErr = fmt.Errorf("failed to remove data at %s: %w", sc.GetPath(path), err)
					cancel()
				})
			}
		})
		start = end
	}
	wg.Wait()
	return resultErr
}

func (sc *StorageClient) deleteObjects(ctx context.Context, batch []types.ObjectIdentifier) error {
	var optFns []func(*s3.Options)
	if sc.compatMode {
		optFns = append(optFns, withContentMD5Checksum)
	}
	resp, err := sc.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(sc.bucket),
		Delete: &types.Delete{
			Objects: batch,
			Quiet:   aws.Bool(true),
		},
	}, optFns...)
	if err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		errors := make(map[string]int)
		for _, e := range resp.Errors {
			if e.Code == nil {
				continue
			}
			code := *e.Code
			if _, ok := errors[code]; !ok {
				errors[code] = 1
			} else {
				errors[code]++
			}
		}
		var errMsg string
		for eName, eCount := range errors {
			if len(errMsg) > 0 {
				errMsg += ", "
			}
			errMsg += fmt.Sprintf("%s - %d", eName, eCount)
		}
		return fmt.Errorf("data was partially removed, encountered errors: %s", errMsg)
	}
	return nil
}

func (sc *StorageClient) Sync(src, dst string) error {
	srcFiles, err := listLocalFiles(src)
	if err != nil {
		return fmt.Errorf("failed to list source files at %s: %w", src, err)
	}
	dstFiles, err := sc.ListFiles(dst)
	if err != nil {
		return fmt.Errorf("failed to list destination files at %s: %w", dst, err)
	}
	var absentFiles []types.ObjectIdentifier
	for dstPath, dstSize := range dstFiles {
		absPath := filepath.Join(src, dstPath)
		if srcSize, ok := srcFiles[absPath]; ok && dstSize == srcSize {
			delete(srcFiles, absPath)
		} else if !ok {
			absentFiles = append(absentFiles, types.ObjectIdentifier{
				Key: aws.String(sc.getPath(filepath.Join(dst, dstPath))),
			})
		}
	}

	var wg sync.WaitGroup
	var start int
	var resultErr error
	var once sync.Once
	concurrencyChan := make(chan struct{}, concurrency)
	ctx, cancel := context.WithCancel(sc.ctx)
	defer cancel()
	for end := maxDeleteObjects; start < len(absentFiles); end += maxDeleteObjects {
		end = min(end, len(absentFiles))
		concurrencyChan <- struct{}{}
		batch := absentFiles[start:end]
		wg.Go(func() {
			defer func() { <-concurrencyChan }()
			if err := sc.deleteObjects(ctx, batch); err != nil {
				once.Do(func() {
					resultErr = fmt.Errorf("failed to remove data at %s: %w", sc.GetPath(dst), err)
					cancel()
				})
			}
		})
		start = end
	}

	for srcPath, srcSize := range srcFiles {
		concurrencyChan <- struct{}{}
		wg.Go(func() {
			defer func() { <-concurrencyChan }()
			fileContent, err := os.Open(srcPath)
			if err != nil {
				logger.Errorf("failed to read source file %s: %s", srcPath, err)
				once.Do(func() {
					resultErr = err
					cancel()
				})
				return
			}
			defer fileContent.Close()
			rel, err := filepath.Rel(src, srcPath)
			if err != nil {
				logger.Errorf("%s is not relative to %s", srcPath, src)
				once.Do(func() {
					resultErr = err
					cancel()
				})
				return
			}
			relKey := filepath.Join(dst, rel)
			_, err = sc.manager.UploadObject(ctx, &transfermanager.UploadObjectInput{
				Bucket: aws.String(sc.bucket),
				Key:    aws.String(sc.getPath(relKey)),
				Body:   fileContent,
			}, singleFileUploadConcurrency)
			if err != nil {
				logger.Errorf("failed to upload to %s: %s", sc.GetPath(relKey), err)
				once.Do(func() {
					resultErr = err
					cancel()
				})
				return
			}
			writtenBytesTotal.Add(int(srcSize))
		})
	}
	wg.Wait()
	return resultErr
}

func (sc *StorageClient) headObject(path string) (*s3.HeadObjectOutput, error) {
	return sc.client.HeadObject(sc.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(sc.getPath(path)),
	})
}

func (sc *StorageClient) objectsToFiles(files map[string]uint64, path string, objects []types.Object) {
	prefix := sc.getPrefix(path)
	for i := range objects {
		o := &objects[i]
		p := *o.Key
		files[p[len(prefix):]] = uint64(*o.Size)
	}
}

func (sc *StorageClient) objectsToDirs(dirs []string, path string, prefixes []types.CommonPrefix) []string {
	prefix := sc.getPrefix(path)
	for i := range prefixes {
		o := &prefixes[i]
		p := *o.Prefix
		dirs = append(dirs, p[len(prefix):len(p)-1])
	}
	return dirs
}

func listLocalFiles(src string) (map[string]uint64, error) {
	files := make(map[string]uint64)
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to access source file=%s: %w", path, err)
		}
		var resolvedPath string
		if d.Type()&fs.ModeSymlink != 0 {
			resolvedPath, err = filepath.EvalSymlinks(path)
			if err != nil {
				if os.IsNotExist(err) {
					// Remove symlink that points to nowhere.
					logger.Infof("removing broken symlink %q", path)
					if err := os.Remove(path); err != nil {
						return fmt.Errorf("cannot remove broken link=%s: %w", path, err)
					}
					return nil
				}
				return fmt.Errorf("cannot resolve symlink=%s: %w", path, err)
			}
		} else if !d.IsDir() {
			resolvedPath = path

		} else {
			return nil
		}
		f, err := os.Stat(resolvedPath)
		if err != nil {
			return fmt.Errorf("cannot get fileinfo file=%s: %w", resolvedPath, err)
		}
		files[path] = uint64(f.Size())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

package testutil

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

type StorageClient struct {
	mu        sync.RWMutex
	files     map[string][]byte
	ReadCalls atomic.Int32
}

func New() *StorageClient {
	return &StorageClient{files: make(map[string][]byte)}
}

func NewWithFiles(files map[string][]byte) *StorageClient {
	sc := New()
	for k, v := range files {
		sc.files[k] = v
	}
	return sc
}

func (c *StorageClient) SetFile(path string, data []byte) {
	c.mu.Lock()
	c.files[path] = data
	c.mu.Unlock()
}

func (c *StorageClient) Close() {}

func (c *StorageClient) CreateFile(path string, data []byte) error {
	c.mu.Lock()
	c.files[path] = data
	c.mu.Unlock()
	return nil
}

func (c *StorageClient) DeletePath(path string, recursive bool) error {
	c.mu.Lock()
	if recursive {
		prefix := path + "/"
		for k := range c.files {
			if strings.HasPrefix(k, prefix) || k == path {
				delete(c.files, k)
			}
		}
	} else {
		delete(c.files, path)
	}
	c.mu.Unlock()
	return nil
}

func (c *StorageClient) GetFileSize(path string) (uint64, error) {
	c.mu.RLock()
	data, ok := c.files[path]
	c.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	return uint64(len(data)), nil
}

func (c *StorageClient) GetPath(path string) string { return "mock://" + path }

func (c *StorageClient) GetRoot() string { return "mock://" }

func (c *StorageClient) IsFileExist(path string) (bool, error) {
	c.mu.RLock()
	_, ok := c.files[path]
	c.mu.RUnlock()
	return ok, nil
}

func (c *StorageClient) IsTransientError(_ error) bool { return false }

func (c *StorageClient) ListFiles(path string) (map[string]uint64, error) {
	prefix := path + "/"
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]uint64)
	for k, v := range c.files {
		if strings.HasPrefix(k, prefix) {
			out[k[len(prefix):]] = uint64(len(v))
		}
	}
	return out, nil
}

func (c *StorageClient) ReadDir(path string) ([]string, error) {
	prefix := path + "/"
	seen := make(map[string]struct{})
	c.mu.RLock()
	for k := range c.files {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		rest := k[len(prefix):]
		if idx := strings.Index(rest, "/"); idx >= 0 {
			seen[rest[:idx]] = struct{}{}
		}
	}
	c.mu.RUnlock()
	dirs := make([]string, 0, len(seen))
	for d := range seen {
		dirs = append(dirs, d)
	}
	return dirs, nil
}

func (c *StorageClient) ReadFile(path string, wr io.WriterAt) error {
	c.mu.RLock()
	data, ok := c.files[path]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	_, err := wr.WriteAt(data, 0)
	return err
}

func (c *StorageClient) ReadRange(path string, buf []byte, offset int64) (int, error) {
	c.ReadCalls.Add(1)
	c.mu.RLock()
	data, ok := c.files[path]
	c.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("%s: %w", path, os.ErrNotExist)
	}
	if offset >= int64(len(data)) {
		return 0, io.EOF
	}
	return copy(buf, data[offset:]), nil
}

func (c *StorageClient) Sync(_, _ string) error { return nil }

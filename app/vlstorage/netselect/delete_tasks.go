package netselect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"golang.org/x/sync/errgroup"
)

// ListDeleteTasks gathers all delete tasks from every storage node and returns them along with the originating storage address.
func (s *Storage) ListDeleteTasks(ctx context.Context, authKey string) ([]logstorage.DeleteTaskInfoWithSource, error) {
	if len(s.sns) == 0 {
		return nil, nil
	}

	g, ctx := errgroup.WithContext(ctx)

	// race-free slices
	results := make([][]logstorage.DeleteTaskInfoWithSource, len(s.sns))
	for i, sn := range s.sns {
		i, sn := i, sn
		g.Go(func() error {
			tasks, err := sn.getDeleteTasks(ctx, authKey)
			if err != nil {
				return err
			}
			results[i] = tasks
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	var all []logstorage.DeleteTaskInfoWithSource
	for _, ts := range results {
		all = append(all, ts...)
	}

	return all, nil
}

func (sn *storageNode) getDeleteTasks(ctx context.Context, authKey string) ([]logstorage.DeleteTaskInfoWithSource, error) {
	args := url.Values{}
	args.Set("version", DeleteProtocolVersion)
	if authKey != "" {
		args.Set("authKey", authKey)
	}

	reqURL := sn.getRequestURLWithArgs("/delete", args)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	if err := sn.ac.SetHeaders(req, true); err != nil {
		return nil, fmt.Errorf("cannot set auth headers for %q: %w", reqURL, err)
	}

	resp, err := sn.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read response body from %q: %w", reqURL, err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code for %q: %d; response: %q", reqURL, resp.StatusCode, body)
	}

	var tasks []logstorage.DeleteTaskInfoWithSource
	if err := json.Unmarshal(body, &tasks); err != nil {
		return nil, fmt.Errorf("cannot decode delete tasks response from %q: %w; response body: %q", reqURL, err, body)
	}

	// Attach origin address.
	for i := range tasks {
		if tasks[i].Storage == "" {
			tasks[i].Storage = sn.addr
		}
	}
	return tasks, nil
}

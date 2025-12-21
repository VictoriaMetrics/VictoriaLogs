package kubernetescollector

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// checkpointsDB manages persistent log file reading state checkpoints.
// It saves reading positions to disk to enable resuming log collection
// after vlagent restarts without data loss or duplication.
//
// The caller is responsible for closing checkpointsDB via stop() method
// when it's no longer needed.
type checkpointsDB struct {
	checkpointsPath string

	checkpoints     map[string]checkpoint
	checkpointsLock sync.Mutex

	wg     sync.WaitGroup
	stopCh chan struct{}
}

// startCheckpointsDB starts a checkpointsDB instance.
// The caller must call stop() when the checkpointsDB is no longer needed.
func startCheckpointsDB(path string) (*checkpointsDB, error) {
	checkpoints, err := readCheckpoints(path)
	if err != nil {
		return nil, err
	}

	checkpointsMap := make(map[string]checkpoint)
	for _, cp := range checkpoints {
		checkpointsMap[cp.Path] = cp
	}

	db := &checkpointsDB{
		checkpointsPath: path,
		checkpoints:     checkpointsMap,
		stopCh:          make(chan struct{}),
	}

	db.startPeriodicSyncCheckpoints()

	return db, nil
}

// checkpoint represents a persistent snapshot of a log file reading state.
//
// The checkpoint is saved to disk to enable resuming log collection from the exact
// position after vlagent restarts, preventing:
//  1. Log duplication when logs are re-read from the beginning.
//  2. Log loss when a log file was rotated while vlagent was down.
//     In this case we should find the rotated file.
//
// checkpoint includes pod metadata (common and stream fields) to allow immediate log processing
// without waiting for the Kubernetes API server to provide pod information.
type checkpoint struct {
	Path   string `json:"path"`
	Inode  uint64 `json:"inode"`
	Offset int64  `json:"offset"`
}

func (db *checkpointsDB) set(cp checkpoint) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	db.checkpoints[cp.Path] = cp
}

func (db *checkpointsDB) get(path string) (checkpoint, bool) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	cp, ok := db.checkpoints[path]
	return cp, ok
}

func (db *checkpointsDB) getAll() []checkpoint {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	cps := make([]checkpoint, 0, len(db.checkpoints))
	for _, cp := range db.checkpoints {
		cps = append(cps, cp)
	}

	return cps
}

func (db *checkpointsDB) delete(path string) {
	db.checkpointsLock.Lock()
	defer db.checkpointsLock.Unlock()

	delete(db.checkpoints, path)
}

func (db *checkpointsDB) mustSync() {
	cps := db.getAll()

	slices.SortFunc(cps, func(a, b checkpoint) int {
		return strings.Compare(a.Path, b.Path)
	})

	data, err := json.MarshalIndent(cps, "", "\t")
	if err != nil {
		logger.Panicf("BUG: cannot marshal checkpoints: %s", err)
	}

	fs.MustWriteAtomic(db.checkpointsPath, data, true)
}

func readCheckpoints(path string) ([]checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Infof("no checkpoints file found at %q; vlagent will read log files from the beginning", path)
			return nil, nil
		}
		return nil, fmt.Errorf("cannot read file checkpoints: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	var checkpoints []checkpoint
	if err := json.Unmarshal(data, &checkpoints); err != nil {
		return nil, fmt.Errorf("cannot unmarshal file checkpoints from %q: %w", path, err)
	}

	return checkpoints, nil
}

// startPeriodicFlushCheckpoints periodically persists in-memory checkpoints to disk.
//
// It complements the explicit sync performed on graceful stop,
// ensuring regular persistence even when the process is killed.
func (db *checkpointsDB) startPeriodicSyncCheckpoints() {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				db.mustSync()
			case <-db.stopCh:
				db.mustSync()
				return
			}
		}
	}()
}

func (db *checkpointsDB) stop() {
	close(db.stopCh)
	db.wg.Wait()
}

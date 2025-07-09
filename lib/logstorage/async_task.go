package logstorage

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// asyncTaskType identifies the type of background (asynchronous) task attached to a partition.
// More types can be added in the future (e.g. compaction, ttl, schema-changes).
type asyncTaskType int

const (
	asyncTaskNone   asyncTaskType = iota // no task
	asyncTaskDelete                      // delete rows matching a query
)

// Status field tracks the outcome of the task.
type asyncTaskStatus int

const (
	taskPending asyncTaskStatus = iota
	taskSuccess
	taskError
)

type asyncTask struct {
	Type      asyncTaskType `json:"type"`
	TenantIDs []TenantID    `json:"tenantIDs,omitempty"` // affected tenants (empty slice = all)
	Query     string        `json:"query,omitempty"`     // serialized LogSQL query
	Seq       uint64        `json:"seq,omitempty"`       // monotonically increasing *global* sequence

	// Status tracks the last execution state; omitted from JSON when zero (pending) to
	// preserve compatibility with tasks created before this field existed.
	Status asyncTaskStatus `json:"status,omitempty"`
}

type asyncTasks struct {
	pt *partition

	lock       sync.Mutex
	ts         []asyncTask
	currentSeq atomic.Uint64
}

func newAsyncTasks(pt *partition, tasks []asyncTask) *asyncTasks {
	res := &asyncTasks{
		pt: pt,
		ts: tasks,
	}
	res.advancePending()

	return res
}

func (at *asyncTasks) advancePending() *asyncTask {
	var result asyncTask

	at.lock.Lock()
	for i := len(at.ts) - 1; i >= 0; i-- {
		task := at.ts[i]
		if task.Status == taskPending {
			result = task
			break
		}
	}
	at.lock.Unlock()

	at.currentSeq.Store(result.Seq)
	return &result
}

func (at *asyncTasks) markResolvedOnDisk(seq uint64, status asyncTaskStatus) {
	at.lock.Lock()
	defer at.lock.Unlock()

	for i := range at.ts {
		if at.ts[i].Seq == seq && at.ts[i].Status == taskPending {
			at.ts[i].Status = status
			at.pt.mustSaveAsyncTasksLocked()
			return
		}
	}
}

// addDeleteTask appends a delete task to the partition's task list
func (at *asyncTasks) addDeleteTask(tenantIDs []TenantID, q *Query, seq uint64) uint64 {
	at.lock.Lock()
	defer at.lock.Unlock()

	task := asyncTask{
		Seq:       seq,
		Type:      asyncTaskDelete,
		TenantIDs: append([]TenantID(nil), tenantIDs...),
		Query:     q.String(),
		Status:    taskPending,
	}

	at.pt.mustSaveAsyncTasksLocked(task)
	return seq
}

// globalTaskSeq provides unique, monotonically increasing sequence numbers for async tasks.
var globalTaskSeq atomic.Uint64

func init() {
	// Initialise with current unix-nano in order to minimise collision with seqs that may be present on disk.
	globalTaskSeq.Store(uint64(time.Now().UnixNano()))
}

// marshalAsyncTasks converts async tasks to JSON for persistence
func marshalAsyncTasks(tasks []asyncTask) []byte {
	data, err := json.Marshal(tasks)
	if err != nil {
		logger.Panicf("FATAL: cannot marshal async tasks: %s", err)
	}
	return data
}

// unmarshalAsyncTasks converts JSON data back to async tasks
func unmarshalAsyncTasks(data []byte) []asyncTask {
	if len(data) == 0 {
		return nil
	}

	var tasks []asyncTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		logger.Panicf("FATAL: cannot unmarshal async tasks: %s", err)
	}
	return tasks
}

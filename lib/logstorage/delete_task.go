package logstorage

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// deleteTaskStatus tracks the current state of a background delete operation.
type deleteTaskStatus string

const (
	deleteTaskPending deleteTaskStatus = "pending"
	deleteTaskSuccess deleteTaskStatus = "success"
	deleteTaskError   deleteTaskStatus = "error"
)

// deleteTask captures all information needed to replay the delete query on parts.
type deleteTask struct {
	TenantIDs   []TenantID       `json:"tenantIDs,omitempty"`
	Query       string           `json:"query"`
	Timestamp   int64            `json:"timestamp,omitempty"`
	Seq         uint64           `json:"seq,omitempty"`
	Status      deleteTaskStatus `json:"status,omitempty"`
	CreatedTime int64            `json:"createdTime,omitempty"`
	DoneTime    int64            `json:"doneTime,omitempty"`
	ErrorMsg    string           `json:"error,omitempty"`
}

// deleteTaskQueue holds the backlog of delete jobs for a partition.
type deleteTaskQueue struct {
	pt *partition

	mu  sync.Mutex
	ts  []deleteTask
	seq atomic.Uint64
}

func newDeleteTaskQueue(pt *partition, tasks []deleteTask) *deleteTaskQueue {
	return &deleteTaskQueue{
		pt: pt,
		ts: tasks,
	}
}

func (dq *deleteTaskQueue) nextPendingTask() deleteTask {
	var result deleteTask

	dq.mu.Lock()
	for i := range dq.ts {
		task := dq.ts[i]
		if task.Status == deleteTaskPending {
			result = task
			break
		}
	}
	dq.mu.Unlock()

	dq.seq.Store(result.Seq)
	return result
}

func (dq *deleteTaskQueue) resolve(seq uint64, err error) {
	status, errMsg := deleteTaskSuccess, ""
	if err != nil {
		status, errMsg = deleteTaskError, err.Error()
	}

	dq.mu.Lock()
	for i := range dq.ts {
		t := &dq.ts[i]

		if t.Seq < seq {
			continue
		}
		if t.Seq > seq || t.Status != deleteTaskPending {
			dq.mu.Unlock()
			return
		}

		t.Status = status
		t.DoneTime = time.Now().UnixNano()
		t.ErrorMsg = errMsg
		dq.mu.Unlock()

		dq.pt.mustSaveDeleteTasks()
		return
	}
	dq.mu.Unlock()
}

func (dq *deleteTaskQueue) add(tenantIDs []TenantID, q *Query, seq uint64) uint64 {
	task := deleteTask{
		Seq:         seq,
		TenantIDs:   tenantIDs,
		Query:       q.String(),
		Timestamp:   q.GetTimestamp(),
		Status:      deleteTaskPending,
		CreatedTime: time.Now().UnixNano(),
	}

	dq.mu.Lock()
	dq.ts = append(dq.ts, task)
	dq.mu.Unlock()

	dq.pt.mustSaveDeleteTasks()
	return seq
}

func unmarshalDeleteTasks(data []byte) ([]deleteTask, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var tasks []deleteTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return nil, fmt.Errorf("unmarshal delete tasks: %w", err)
	}
	return tasks, nil
}

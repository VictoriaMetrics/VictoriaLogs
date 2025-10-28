package logstorage

import (
	"strings"
	"sync"
	"time"
)

// DeleteTaskInfo represents brief information about a background delete task.
type DeleteTaskInfo struct {
	Seq         uint64           `json:"seq"`
	Status      deleteTaskStatus `json:"status"`
	Tenant      string           `json:"tenant"`
	Query       string           `json:"query"`
	CreatedTime int64            `json:"createdTime,omitempty"`
	DoneTime    int64            `json:"doneTime,omitempty"`
	Error       string           `json:"error,omitempty"`
}

// DeleteTaskInfoWithSource extends DeleteTaskInfo with metadata about the storage node it originated from.
type DeleteTaskInfoWithSource struct {
	DeleteTaskInfo `json:",inline"`
	Storage        string `json:"storage"`
}

var deleteTasksCache struct {
	mu   sync.Mutex
	ts   time.Time
	data []DeleteTaskInfo
}

// ListDeleteTasks gathers information about all delete tasks known to this Storage instance.
// The returned slice isn't sorted.
func (s *Storage) ListDeleteTasks() []DeleteTaskInfo {
	deleteTasksCache.mu.Lock()
	const cacheTTL = 5 * time.Second
	if time.Since(deleteTasksCache.ts) < cacheTTL && deleteTasksCache.data != nil {
		d := append([]DeleteTaskInfo(nil), deleteTasksCache.data...)
		deleteTasksCache.mu.Unlock()
		return d
	}
	deleteTasksCache.mu.Unlock()

	s.partitionsLock.Lock()
	pws := append([]*partitionWrapper(nil), s.partitions...)
	for _, pw := range pws {
		pw.incRef()
	}
	s.partitionsLock.Unlock()
	defer func() {
		for _, p := range pws {
			p.decRef()
		}
	}()

	var out []DeleteTaskInfo
	seqToIdx := make(map[uint64]int)
	merge := func(dst *DeleteTaskInfo, src *DeleteTaskInfo) {
		prioritize := func(s deleteTaskStatus) int {
			switch s {
			case deleteTaskError:
				return 3
			case deleteTaskPending:
				return 2
			case deleteTaskSuccess:
				return 1
			}
			return 0
		}
		if prioritize(src.Status) > prioritize(dst.Status) {
			dst.Status = src.Status
			dst.Error = src.Error
		}
		if dst.CreatedTime == 0 || (src.CreatedTime != 0 && src.CreatedTime < dst.CreatedTime) {
			dst.CreatedTime = src.CreatedTime
		}
		if src.DoneTime > dst.DoneTime {
			dst.DoneTime = src.DoneTime
		}
	}

	for _, p := range pws {
		dq := p.pt.deleteQueue

		dq.mu.Lock()
		tasks := append([]deleteTask(nil), dq.ts...)
		dq.mu.Unlock()

		for _, t := range tasks {
			info := deleteTaskToInfo(t)
			if idx, ok := seqToIdx[info.Seq]; ok {
				merge(&out[idx], &info)
				continue
			}
			seqToIdx[info.Seq] = len(out)
			out = append(out, info)
		}
	}

	deleteTasksCache.mu.Lock()
	deleteTasksCache.ts = time.Now()
	deleteTasksCache.data = out
	deleteTasksCache.mu.Unlock()

	return out
}

func deleteTaskToInfo(t deleteTask) DeleteTaskInfo {
	tn := "*"
	if len(t.TenantIDs) > 0 {
		var b strings.Builder
		for i, id := range t.TenantIDs {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(id.String())
		}
		tn = b.String()
	}
	return DeleteTaskInfo{
		Seq:         t.Seq,
		Status:      t.Status,
		Tenant:      tn,
		Query:       t.Query,
		CreatedTime: t.CreatedTime,
		DoneTime:    t.DoneTime,
		Error:       t.ErrorMsg,
	}
}

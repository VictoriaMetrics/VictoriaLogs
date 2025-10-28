package logstorage

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// startDeleteTaskWorker launches a background goroutine, which periodically
// scans partitions for parts lagging behind delete tasks and applies these
// tasks by re-executing their underlying queries via MarkRows (with
// createTask=false).
func (s *Storage) startDeleteTaskWorker() {
	const interval = 5 * time.Second
	const maxFailure = 3

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-s.stopCh
		cancel()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for failedTime := 0; ; {
			select {
			case <-s.stopCh:
				return
			case <-time.After(interval):
				// Honour pause requests, if any. If cannot process, just reset timer and continue.
				if s.deleteTaskState.isPaused() {
					continue
				}

				seq, err := s.executeDeleteTasksOnce(ctx)
				if err != nil {
					logger.Errorf("delete task worker: %s", err)
					failedTime++
				} else {
					failedTime = 0
				}

				if failedTime > maxFailure {
					s.setTaskFailed(seq, err)
					failedTime = 0
				}
			}
		}
	}()
}

// executeDeleteTasksOnce performs a single pass over all partitions (latest → oldest)
// and applies pending delete tasks to every part that hasn't caught up yet.
func (s *Storage) executeDeleteTasksOnce(ctx context.Context) (uint64, error) {
	var seq uint64

	// Snapshot partitions (most recent first).
	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper{}, s.partitions...)
	for _, ptw := range ptws {
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	defer func() {
		for _, ptw := range ptws {
			ptw.decRef()
		}
	}()

	outdatedPtws, task := s.findNextDeleteTask(ptws)
	if task.Seq == 0 {
		return 0, nil
	}

	seq = task.Seq

	// Gather all lagging parts in the target partition for this sequence.
	lagging := []*partWrapper{}
	pending := 0
	for _, ptw := range outdatedPtws {
		pt := ptw.pt
		pt.ddb.partsLock.Lock()
		allPws := [][]*partWrapper{pt.ddb.inmemoryParts, pt.ddb.smallParts, pt.ddb.bigParts}
		for _, arr := range allPws {
			for _, pw := range arr {
				if pw.taskSeq.Load() >= task.Seq {
					continue
				}

				if pw.isInMerge {
					pending++
					continue
				}

				if pw.mustDrop.Load() {
					continue
				}

				pw.incRef()
				lagging = append(lagging, pw)
			}
		}
		pt.ddb.partsLock.Unlock()
	}

	// If there are no lagging parts and no pending parts,
	// mark the task as successful and return.
	if len(lagging) == 0 {
		if pending > 0 {
			return seq, nil
		}

		s.setTaskComplete(outdatedPtws, task.Seq, false, nil)
		return seq, nil
	}
	defer func() {
		for _, pw := range lagging {
			pw.decRef()
		}
	}()

	if err := s.processDeleteTask(ctx, task, lagging); err != nil {
		return 0, fmt.Errorf("run delete task: %w", err)
	}

	for _, pw := range lagging {
		pw.taskSeq.Store(task.Seq)
	}

	if pending == 0 {
		s.setTaskComplete(outdatedPtws, task.Seq, false, nil)
	}

	return seq, nil
}

func (s *Storage) setTaskFailed(sequence uint64, err error) {
	if sequence == 0 || err == nil {
		return
	}

	// Take a snapshot of partitions
	s.partitionsLock.Lock()
	ptws := append([]*partitionWrapper{}, s.partitions...)
	for _, ptw := range ptws {
		ptw.incRef()
	}
	s.partitionsLock.Unlock()

	// Mark the tasks as error for partitions and parts
	s.setTaskComplete(ptws, sequence, true, err)
	for _, ptw := range ptws {
		ptw.decRef()
	}
}

func (s *Storage) setTaskComplete(ptws []*partitionWrapper, taskSeq uint64, includeParts bool, err error) {
	for _, ptw := range ptws {
		pt := ptw.pt

		if includeParts {
			// Ensure every part in this partition has taskSeq at least `sequence`.
			pt.ddb.partsLock.Lock()
			all := [][]*partWrapper{pt.ddb.inmemoryParts, pt.ddb.smallParts, pt.ddb.bigParts}
			for _, arr := range all {
				for _, pw := range arr {
					if pw.taskSeq.Load() < taskSeq {
						pw.taskSeq.Store(taskSeq)
					}
				}
			}
			pt.ddb.partsLock.Unlock()
		}

		pt.deleteQueue.resolve(taskSeq, err)
	}
}

func (s *Storage) processDeleteTask(ctx context.Context, task deleteTask, lagging []*partWrapper) error {
	// Build allowed set
	allowed := make(map[*partition][]*partWrapper, len(lagging))
	for _, pw := range lagging {
		allowed[pw.p.pt] = append(allowed[pw.p.pt], pw)
	}

	err := s.markDeleteRowsOnParts(ctx, task.TenantIDs, task.Query, task.Timestamp, task.Seq, allowed)
	if err != nil {
		return fmt.Errorf("failed to mark delete rows on parts: %w", err)
	}

	return nil
}

func (s *Storage) findNextDeleteTask(ptws []*partitionWrapper) ([]*partitionWrapper, deleteTask) {
	var minSeq uint64 = math.MaxUint64
	var result deleteTask
	var resultPtws []*partitionWrapper

	for _, ptw := range ptws {
		pt := ptw.pt

		task := pt.deleteQueue.nextPendingTask()
		if task.Seq == 0 || task.Seq > minSeq {
			continue
		}

		// If we find a smaller sequence,
		// reset the slice to start a new collection.
		if task.Seq < minSeq {
			minSeq = task.Seq
			resultPtws = append(resultPtws[:0], ptw)
			result = task
			continue
		}

		resultPtws = append(resultPtws, ptw)
	}

	s.deleteTaskSeq.Store(result.Seq)
	return resultPtws, result
}

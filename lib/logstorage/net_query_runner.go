package logstorage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// RunNetQueryFunc must run qctx and pass the query results to writeBlock.
type RunNetQueryFunc func(qctx *QueryContext, writeBlock WriteDataBlockFunc) error

// NetQueryRunner is a runner for distributed query.
type NetQueryRunner struct {
	// qctx is the query context.
	qctx *QueryContext

	// qRemote is the query to execute at remote storage nodes.
	qRemote *Query

	// pipesLocal are pipes to execute locally after receiving the data from remote storage nodes.
	pipesLocal []pipe

	// writeBlock is the function for writing the resulting data block.
	writeBlock writeBlockResultFunc

	// memReserved is the amount of memory reserved by subqueries
	memReserved uint64
}

// NewNetQueryRunner creates a new NetQueryRunner for the given qctx.
//
// runNetQuery is used for running distributed query.
// qctx results are sent to writeNetBlock.
//
// The caller must call MustReleaseMemory on the returned runner when it is no longer needed,
// typically via defer, in order to release the memory reserved for subqueries.
func NewNetQueryRunner(qctx *QueryContext, runNetQuery RunNetQueryFunc, writeNetBlock WriteDataBlockFunc) (*NetQueryRunner, error) {
	runQuery := func(qctx *QueryContext, writeBlock writeBlockResultFunc) error {
		writeNetBlock := writeBlock.newDataBlockWriter()
		return runNetQuery(qctx, writeNetBlock)
	}

	qRemote, pipesLocal := splitQueryToRemoteAndLocal(qctx.Query)

	var memReserved uint64

	// Eagerly execute all the subqueries for the remote query
	// and replace them with the query results directly in qRemote.
	// This is needed for proper propagation subquery results to remote storage nodes.
	qctxRemote := qctx.WithQuery(qRemote)
	qRemote, mem, err := initSubqueries(qctxRemote, runQuery, true)
	memReserved += mem // subqueries initialization might fail, but some mem is already reserved
	if err != nil {
		getQueryMemoryLimiter().Put(memReserved)
		return nil, err
	}

	// Initialize subqueries inside local parts.
	// There is no need in eager execution of all the subqueries such as union (...)
	// since it is OK to execute them lazily.
	qLocal, err := ParseQuery("*")
	if err != nil {
		logger.Panicf("BUG: cannot parse '*' query: %s", err)
	}
	qLocal.pipes = pipesLocal
	qctxLocal := qctx.WithQuery(qLocal)
	qLocal, mem, err = initSubqueries(qctxLocal, runQuery, false)
	memReserved += mem
	if err != nil {
		getQueryMemoryLimiter().Put(memReserved)
		return nil, err
	}

	writeBlock := writeNetBlock.newBlockResultWriter()

	nqr := &NetQueryRunner{
		qctx:        qctx,
		qRemote:     qRemote,
		pipesLocal:  qLocal.pipes,
		writeBlock:  writeBlock,
		memReserved: memReserved,
	}
	return nqr, nil
}

// MustReleaseMemory returns the memory reserved for subqueries back to the global query memory limiter.
//
// It must be called after NewNetQueryRunner returns successfully, typically via defer,
// even if Run isn't called; otherwise the reserved memory leaks.
func (nqr *NetQueryRunner) MustReleaseMemory() {
	getQueryMemoryLimiter().Put(nqr.memReserved)
	nqr.memReserved = 0
}

// Run runs the nqr query.
//
// The concurrency limits the number of concurrent goroutines, which process the query results at the local host.
//
// netSearch must execute the given query q at remote storage nodes and pass results to writeBlock.
func (nqr *NetQueryRunner) Run(ctx context.Context, concurrency int, netSearch func(stopCh <-chan struct{}, q *Query, writeBlock WriteDataBlockFunc) error) error {
	search := func(stopCh <-chan struct{}, writeBlockToPipes writeBlockResultFunc) error {
		writeNetBlock := writeBlockToPipes.newDataBlockWriter()
		return netSearch(stopCh, nqr.qRemote, writeNetBlock)
	}

	qctxLocal := nqr.qctx.WithContext(ctx)
	return runPipes(qctxLocal, nqr.pipesLocal, search, nqr.writeBlock, concurrency)
}

// splitQueryToRemoteAndLocal splits q into remotely executed query and into locally executed pipes.
func splitQueryToRemoteAndLocal(q *Query) (*Query, []pipe) {
	timestamp := q.GetTimestamp()
	qRemote := q.Clone(timestamp)
	qRemote.enablePrintOptions()

	pipesRemote, pipesLocal := getRemoteAndLocalPipes(qRemote)
	qRemote.DropAllPipes()
	qRemote.pipes = pipesRemote

	if !qRemote.IsFixedOutputFieldsOrder() {
		// Limit fields to select at the remote storage if the output fields aren't fixed.
		pf := getNeededColumns(pipesLocal)
		qRemote.addFieldsFilters(pf)
	}

	return qRemote, pipesLocal
}

func getRemoteAndLocalPipes(q *Query) ([]pipe, []pipe) {
	timestamp := q.GetTimestamp()

	var pipesRemote []pipe
	var pipesLocal []pipe

	for i, p := range q.pipes {
		pRemote, psLocal := p.splitToRemoteAndLocal(timestamp)
		if pRemote != nil {
			pipesRemote = append(pipesRemote, pRemote)
			if len(psLocal) == 0 {
				continue
			}
		}

		if len(psLocal) == 0 {
			logger.Panicf("BUG: psLocal must be non non-empty here")
		}

		pipesLocal = append(pipesLocal, psLocal...)
		pipesLocal = append(pipesLocal, q.pipes[i+1:]...)
		break
	}

	return pipesRemote, pipesLocal
}

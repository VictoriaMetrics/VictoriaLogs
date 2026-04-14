package logstorage

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestStorageLifecycle(t *testing.T) {
	t.Parallel()

	path := t.Name()

	for range 3 {
		cfg := &StorageConfig{}
		s := MustOpenStorage(path, cfg)
		s.MustClose()
	}
	fs.MustRemoveDir(path)
}

func TestStorageMustAddRows(t *testing.T) {
	t.Parallel()

	path := t.Name()

	cfg := &StorageConfig{}
	s := MustOpenStorage(path, cfg)

	// Try adding the same entry multiple times.
	totalRowsCount := uint64(0)
	for range 100 {
		lr := newTestLogRows(1, 1, 0)
		lr.timestamps[0] = time.Now().UTC().UnixNano()
		totalRowsCount += uint64(len(lr.timestamps))
		s.MustAddRows(lr)
	}
	s.DebugFlush()

	var sStats StorageStats
	s.UpdateStats(&sStats)
	if n := sStats.RowsCount(); n != totalRowsCount {
		t.Fatalf("unexpected number of entries in storage; got %d; want %d", n, totalRowsCount)
	}

	s.MustClose()

	// Re-open the storage and try writing data to it
	s = MustOpenStorage(path, cfg)

	sStats.Reset()
	s.UpdateStats(&sStats)
	if n := sStats.RowsCount(); n != totalRowsCount {
		t.Fatalf("unexpected number of entries in storage; got %d; want %d", n, totalRowsCount)
	}

	lr := newTestLogRows(3, 10, 0)
	for i := range lr.timestamps {
		lr.timestamps[i] = time.Now().UTC().UnixNano()
	}
	totalRowsCount += uint64(len(lr.timestamps))
	s.MustAddRows(lr)
	s.DebugFlush()
	sStats.Reset()
	s.UpdateStats(&sStats)
	if n := sStats.RowsCount(); n != totalRowsCount {
		t.Fatalf("unexpected number of entries in storage; got %d; want %d", n, totalRowsCount)
	}

	s.MustClose()

	// Re-open the storage with big retention and try writing data
	// to different days in the past and in the future
	cfg = &StorageConfig{
		Retention:       365 * 24 * time.Hour,
		FutureRetention: 365 * 24 * time.Hour,
	}
	s = MustOpenStorage(path, cfg)

	lr = newTestLogRows(3, 10, 0)
	now := time.Now().UTC().UnixNano() - int64(len(lr.timestamps)/2)*nsecsPerDay
	for i := range lr.timestamps {
		lr.timestamps[i] = now
		now += nsecsPerDay
	}
	totalRowsCount += uint64(len(lr.timestamps))
	s.MustAddRows(lr)
	s.DebugFlush()
	sStats.Reset()
	s.UpdateStats(&sStats)
	if n := sStats.RowsCount(); n != totalRowsCount {
		t.Fatalf("unexpected number of entries in storage; got %d; want %d", n, totalRowsCount)
	}

	s.MustClose()

	// Make sure the stats is valid after re-opening the storage
	s = MustOpenStorage(path, cfg)
	sStats.Reset()
	s.UpdateStats(&sStats)
	if n := sStats.RowsCount(); n != totalRowsCount {
		t.Fatalf("unexpected number of entries in storage; got %d; want %d", n, totalRowsCount)
	}
	s.MustClose()

	fs.MustRemoveDir(path)
}

func TestStoragePartitionDetachRecreateSameDaySameStream(t *testing.T) {
	t.Parallel()

	path := t.Name()

	cfg := &StorageConfig{
		Retention:       365 * 24 * time.Hour,
		FutureRetention: 365 * 24 * time.Hour,
	}
	s := MustOpenStorage(path, cfg)

	tenantIDs := []TenantID{{}}
	ts := time.Now().UTC().UnixNano()
	partitionName := getPartitionNameFromDay(ts / nsecsPerDay)

	addRow := func(stream, marker, msg string) {
		t.Helper()

		lr := GetLogRows([]string{"stream"}, nil, nil, nil, "")
		lr.mustAdd(TenantID{}, ts, []Field{
			{
				Name:  "stream",
				Value: stream,
			},
			{
				Name:  "marker",
				Value: marker,
			},
			{
				Name:  "_msg",
				Value: msg,
			},
		})
		s.MustAddRows(lr)
		PutLogRows(lr)
	}

	check := func(qStr string, rowsExpected int) {
		t.Helper()
		checkQueryResults(t, s, ts, tenantIDs, qStr, nil, []string{fmt.Sprintf(`{"rows":"%d"}`, rowsExpected)})
	}

	addRow("same_stream", "before_detach", "before detach")
	s.DebugFlush()
	check(`marker:=before_detach | stats count(*) as rows`, 1)

	if err := s.PartitionDetach(partitionName); err != nil {
		t.Fatalf("cannot detach partition %q: %s", partitionName, err)
	}
	fs.MustRemoveDir(filepath.Join(path, partitionsDirname, partitionName))

	addRow("same_stream", "after_detach", "after detach")
	s.DebugFlush()

	check(`marker:=after_detach | stats count(*) as rows`, 1)
	check(`* | stats count(*) as rows`, 1)

	s.MustClose()
	fs.MustRemoveDir(path)
}

func TestStoragePartitionDetachRecreateSameDayStreamFilterQuery(t *testing.T) {
	t.Parallel()

	path := t.Name()

	cfg := &StorageConfig{
		Retention: 365 * 24 * time.Hour,
	}
	s := MustOpenStorage(path, cfg)

	tenantIDs := []TenantID{{}}
	ts := time.Now().UTC().UnixNano()
	partitionName := getPartitionNameFromDay(ts / nsecsPerDay)

	addRow := func(stream, marker, msg string) {

		lr := GetLogRows([]string{"stream"}, nil, nil, nil, "")
		lr.mustAdd(TenantID{}, ts, []Field{
			{
				Name:  "stream",
				Value: stream,
			},
			{
				Name:  "marker",
				Value: marker,
			},
			{
				Name:  "_msg",
				Value: msg,
			},
		})
		s.MustAddRows(lr)
		PutLogRows(lr)
	}

	check := func(qStr string, rowsExpected int) {
		t.Helper()
		checkQueryResults(t, s, ts, tenantIDs, qStr, nil, []string{fmt.Sprintf(`{"rows":"%d"}`, rowsExpected)})
	}

	addRow("same_stream", "before_detach", "before detach")
	s.DebugFlush()

	// Populate filterStreamCache with an empty result for new_stream at the old partition.
	check(`{stream="new_stream"} | stats count(*) as rows`, 0)

	if err := s.PartitionDetach(partitionName); err != nil {
		t.Fatalf("cannot detach partition %q: %s", partitionName, err)
	}
	fs.MustRemoveDir(filepath.Join(path, partitionsDirname, partitionName))

	addRow("new_stream", "after_detach", "after detach")
	s.DebugFlush()

	check(`{stream="new_stream"} | stats count(*) as rows`, 1)

	s.MustClose()
	fs.MustRemoveDir(path)
}

func TestStorageDeleteTaskOps(t *testing.T) {
	t.Parallel()

	path := t.Name()
	cfg := &StorageConfig{}
	s := MustOpenStorage(path, cfg)

	ctx := t.Context()
	taskID := "task_id_1"
	timestamp := int64(1234567890123456789)
	tenantIDs := []TenantID{
		{
			AccountID: 123,
			ProjectID: 456,
		},
	}
	f, err := ParseFilter("app:=foo _msg:SECRET")
	if err != nil {
		t.Fatalf("cannot parse filter: %s", err)
	}

	// Register delete task
	if err := s.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f); err != nil {
		t.Fatalf("unexpected error in DeleteRunTask: %s", err)
	}

	// Verify that the delete task is registered
	dts, err := s.DeleteActiveTasks(ctx)
	if err != nil {
		t.Fatalf("unexpected error in DeleteActiveTasks: %s", err)
	}
	result := MarshalDeleteTasksToJSON(dts)
	resultExpected := `[{"task_id":"task_id_1","tenant_ids":[{"account_id":123,"project_id":456}],"filter":"app:=foo SECRET","start_time":"2009-02-13T23:31:30.123456789Z"}]`
	if string(result) != resultExpected {
		t.Fatalf("unexpected result\ngot\n%s\nwant\n%s", result, resultExpected)
	}

	// Stop the registered delete task
	if err := s.DeleteStopTask(ctx, taskID); err != nil {
		t.Fatalf("cannot stop the delete task: %s", err)
	}

	// Verify that the list of delete tasks is empty
	dts, err = s.DeleteActiveTasks(ctx)
	if err != nil {
		t.Fatalf("unexpected error in DeleteActiveTasks: %s", err)
	}
	if len(dts) > 0 {
		t.Fatalf("unexpected number of deleted tasks: %d; want 0; tasks: %s", len(dts), MarshalDeleteTasksToJSON(dts))
	}

	s.MustClose()

	fs.MustRemoveDir(path)
}

func TestStorageProcessDeleteTask(t *testing.T) {
	t.Parallel()

	path := t.Name()
	ctx := t.Context()

	cfg := &StorageConfig{
		Retention: 30 * 24 * time.Hour,
	}
	s := MustOpenStorage(path, cfg)

	now := time.Now().UnixNano()

	check := func(tenantIDs []TenantID, filters string, rowsExpected []string) {
		t.Helper()
		checkQueryResults(t, s, now, tenantIDs, filters, nil, rowsExpected)
	}

	deleteRows := func(tenantIDs []TenantID, filters string) {
		t.Helper()
		dt := newDeleteTask("task_id_x", now, tenantIDs, filters)
		for !s.processDeleteTask(ctx, dt) {
			// Unsuccessful attempt because of concurrently executed background merges.
			// Wait for a bit and try again.
			time.Sleep(10 * time.Millisecond)
		}
	}

	allTenantIDs := []TenantID{
		{
			AccountID: 0,
			ProjectID: 100,
		},
		{
			AccountID: 123,
			ProjectID: 0,
		},
		{
			AccountID: 123,
			ProjectID: 456,
		},
	}

	storeRowsForProcessDeleteTaskTest(s, allTenantIDs, now)

	// Verify that all the rows are properly stored across all the tenants
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"10500"}`})
	for i := range allTenantIDs {
		checkQueryResults(t, s, now, []TenantID{allTenantIDs[i]}, "* | count(host) rows", nil, []string{`{"rows":"3500"}`})
	}
	check([]TenantID{allTenantIDs[0], allTenantIDs[2]}, "* | count(host) rows", []string{`{"rows":"7000"}`})

	// Try deleting non-existing logs
	check(allTenantIDs, "row_id:=foobar | count(host) rows", []string{`{"rows":"0"}`})
	deleteRows(allTenantIDs, "row_id:=foobar")
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"10500"}`})

	// Delete logs with the given row_id across all the tenants
	check(allTenantIDs, "row_id:=42 | count(host) rows", []string{`{"rows":"105"}`})
	deleteRows(allTenantIDs, "row_id:=42")
	check(allTenantIDs, "row_id:=42 | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "row_id:!=42 | count(host) rows", []string{`{"rows":"10395"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"10395"}`})

	// Delete logs for the given row_id at two tenants
	tenantIDs := []TenantID{
		allTenantIDs[0],
		allTenantIDs[2],
	}
	check(allTenantIDs, "row_id:=10 | count(host) rows", []string{`{"rows":"105"}`})
	check(tenantIDs, "row_id:=10 | count(host) rows", []string{`{"rows":"70"}`})
	deleteRows(tenantIDs, "row_id:=10")
	check(tenantIDs, "row_id:=10 | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "row_id:=10 | count(host) rows", []string{`{"rows":"35"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"10325"}`})

	// Delete all the logs for the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[1],
	}
	check(tenantIDs, "* | count(host) rows", []string{`{"rows":"3465"}`})
	deleteRows(tenantIDs, "*")
	check(tenantIDs, "* | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"6860"}`})

	// Delete all the logs for the particular day
	filter := "_time:1d offset 2d"
	check(allTenantIDs, filter+" | count(host) rows", []string{`{"rows":"980"}`})
	deleteRows(allTenantIDs, filter)
	check(allTenantIDs, filter+" | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"5880"}`})

	// Delete logs by _stream filter at the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[0],
	}
	filter = `{host="host-4",app=~"app-.+"}`
	check(tenantIDs, filter+" | count(host) rows", []string{`{"rows":"588"}`})
	deleteRows(tenantIDs, filter)
	check(tenantIDs, filter+" | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"5292"}`})

	// Delete logs by composite filter at the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[2],
	}
	filter = `(_msg:3 row_id:23 _time:2d) or (row_id:56 {host=~"host-[23]"} app:*02* tenant_id:~"56")`
	check(tenantIDs, filter+" | count(host) rows", []string{`{"rows":"8"}`})
	deleteRows(tenantIDs, filter)
	check(tenantIDs, filter+" | count(host) rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count(host) rows", []string{`{"rows":"5284"}`})

	s.MustClose()

	fs.MustRemoveDir(path)
}

func TestStorageProcessDeleteTaskRelativeTimeUsesTaskStartTime(t *testing.T) {
	t.Parallel()

	path := t.Name()
	ctx := t.Context()

	cfg := &StorageConfig{
		Retention:       30 * 24 * time.Hour,
		FutureRetention: 30 * 24 * time.Hour,
	}
	s := MustOpenStorage(path, cfg)

	tenantIDs := []TenantID{
		{
			AccountID: 123,
			ProjectID: 456,
		},
	}

	now := time.Now().UnixNano() - int64(2*time.Second)
	rowTimestamp := now - int64(500*time.Millisecond)

	lr := GetLogRows([]string{"host"}, nil, nil, nil, "")
	lr.MustAdd(tenantIDs[0], rowTimestamp, []Field{
		{
			Name:  "host",
			Value: "host-1",
		},
		{
			Name:  "row_id",
			Value: "1",
		},
	}, -1)
	s.MustAddRows(lr)
	PutLogRows(lr)
	s.DebugFlush()

	check := func(qStr string, resultsExpected []string) {
		t.Helper()
		checkQueryResults(t, s, now, tenantIDs, qStr, nil, resultsExpected)
	}

	check(`row_id:=1 | stats count(*) as rows`, []string{`{"rows":"1"}`})

	dt := newDeleteTask("task_id_relative", now, tenantIDs, `_time:1s row_id:=1`)
	for !s.processDeleteTask(ctx, dt) {
		time.Sleep(10 * time.Millisecond)
	}

	check(`row_id:=1 | stats count(*) as rows`, []string{`{"rows":"0"}`})

	s.MustClose()
	fs.MustRemoveDir(path)
}

func checkQueryResults(t *testing.T, s *Storage, now int64, tenantIDs []TenantID, qStr string, hiddenFieldsFilters, resultsExpected []string) {
	t.Helper()

	q, err := ParseQueryAtTimestamp(qStr, now)
	if err != nil {
		t.Fatalf("cannot parse query %q: %s", qStr, err)
	}

	ctx := t.Context()
	var qs QueryStats
	qctx := NewQueryContext(ctx, &qs, tenantIDs, q, false, hiddenFieldsFilters)

	var buf []byte
	var bufLock sync.Mutex

	callback := func(_ uint, db *DataBlock) {
		rows := make([][]Field, db.RowsCount())

		columns := db.GetColumns(false)
		for _, c := range columns {
			for rowID, v := range c.Values {
				rows[rowID] = append(rows[rowID], Field{
					Name:  c.Name,
					Value: v,
				})
			}
		}

		bufLock.Lock()
		for _, r := range rows {
			buf = MarshalFieldsToJSON(buf, r)
			buf = append(buf, '\n')
		}
		bufLock.Unlock()
	}

	if err := s.RunQuery(qctx, callback); err != nil {
		t.Fatalf("unexpected error while running query %q for tenants %s: %s", q, tenantIDs, err)
	}

	if len(buf) > 0 {
		// Drop the last \n
		buf = buf[:len(buf)-1]
	}
	resultsStr := string(buf)
	resultsStrExpected := strings.Join(resultsExpected, "\n")
	if resultsStr != resultsStrExpected {
		t.Fatalf("unexpected results for query %q at tenants %s\ngot\n%s\nwant\n%s", q, tenantIDs, resultsStr, resultsStrExpected)
	}
}

func storeRowsForProcessDeleteTaskTest(s *Storage, tenantIDs []TenantID, now int64) {
	// Generate rows and put them in the storage

	streamTags := []string{
		"host",
		"app",
	}

	lr := GetLogRows(streamTags, nil, nil, nil, "")
	var fields []Field

	const days = 7
	const streamsPerTenant = 5
	const rowsPerDayPerStream = 100

	for rowID := range rowsPerDayPerStream {
		for streamID := range streamsPerTenant {
			fields = append(fields[:0], Field{
				Name:  "host",
				Value: fmt.Sprintf("host-%d", streamID),
			}, Field{
				Name:  "app",
				Value: fmt.Sprintf("app-%d", 200+streamID),
			})
			for _, tenantID := range tenantIDs {
				for dayID := range int64(days) {
					fields = append(fields, Field{
						Name:  "_msg",
						Value: fmt.Sprintf("value #%d at the day %d for the tenantID=%s and streamID=%d", rowID, dayID, tenantID, streamID),
					}, Field{
						Name:  "row_id",
						Value: fmt.Sprintf("%d", rowID),
					}, Field{
						Name:  "tenant_id",
						Value: tenantID.String(),
					})
					timestamp := now - dayID*nsecsPerDay
					lr.mustAdd(tenantID, timestamp, fields)
					if lr.NeedFlush() {
						s.MustAddRows(lr)
						lr.ResetKeepSettings()
					}
				}
			}
		}
	}
	s.MustAddRows(lr)
	PutLogRows(lr)

	s.DebugFlush()
}

func TestStorageDropStalePartitions(t *testing.T) {
	t.Parallel()

	path := t.Name()

	cfg := &StorageConfig{
		Retention: 30 * 24 * time.Hour,
	}
	s := MustOpenStorage(path, cfg)

	expectPartitionsNumber := func(n int) {
		t.Helper()

		pws := s.getPartitions()
		defer s.putPartitions(pws)

		if len(pws) != n {
			t.Fatalf("unexpected number of partitions; got %d; want %d", len(pws), n)
		}
	}

	var tenantID TenantID
	timestamp := time.Now().UnixNano() - 10*nsecsPerDay
	timestamp -= timestamp % nsecsPerDay
	lr := GetLogRows(nil, nil, nil, nil, "")
	for i := range 100 {
		fields := []Field{
			{
				Name:  "_msg",
				Value: fmt.Sprintf("message #%d", i),
			},
		}
		timestamp += nsecsPerSecond
		lr.mustAdd(tenantID, timestamp, fields)
	}

	s.dropStalePartitions()
	expectPartitionsNumber(0)
	s.MustAddRows(lr)
	PutLogRows(lr)
	s.DebugFlush()
	s.dropStalePartitions()
	expectPartitionsNumber(1)
	s.MustClose()

	// Open the storage with the same retention and verify partitions still exsit
	s = MustOpenStorage(path, cfg)
	expectPartitionsNumber(1)
	s.MustClose()

	// Open the storage with smaller retention and drop stale partitions
	cfg = &StorageConfig{
		Retention: 24 * time.Hour,
	}
	s = MustOpenStorage(path, cfg)
	s.dropStalePartitions()
	expectPartitionsNumber(0)
	s.MustClose()

	// Drop the created data on disk
	fs.MustRemoveDir(path)
}

package logstorage

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestStorageLifecycle(t *testing.T) {
	t.Parallel()

	path := t.Name()

	for i := 0; i < 3; i++ {
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
	for i := 0; i < 100; i++ {
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
		checkQueryResults(t, s, tenantIDs, filters, nil, rowsExpected)
	}

	deleteRows := func(tenantIDs []TenantID, filters string) {
		t.Helper()
		dt := newDeleteTask("task_id_x", tenantIDs, filters, now)
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
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"10500"}`})
	for i := range allTenantIDs {
		checkQueryResults(t, s, []TenantID{allTenantIDs[i]}, "* | count() rows", nil, []string{`{"rows":"3500"}`})
	}
	check([]TenantID{allTenantIDs[0], allTenantIDs[2]}, "* | count() rows", []string{`{"rows":"7000"}`})

	// Try deleting non-existing logs
	check(allTenantIDs, "row_id:=foobar | count() rows", []string{`{"rows":"0"}`})
	deleteRows(allTenantIDs, "row_id:=foobar")
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"10500"}`})

	// Delete logs with the given row_id across all the tenants
	check(allTenantIDs, "row_id:=42 | count() rows", []string{`{"rows":"105"}`})
	deleteRows(allTenantIDs, "row_id:=42")
	check(allTenantIDs, "row_id:=42 | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "row_id:!=42 | count() rows", []string{`{"rows":"10395"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"10395"}`})

	// Delete logs for the given row_id at two tenants
	tenantIDs := []TenantID{
		allTenantIDs[0],
		allTenantIDs[2],
	}
	check(allTenantIDs, "row_id:=10 | count() rows", []string{`{"rows":"105"}`})
	check(tenantIDs, "row_id:=10 | count() rows", []string{`{"rows":"70"}`})
	deleteRows(tenantIDs, "row_id:=10")
	check(tenantIDs, "row_id:=10 | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "row_id:=10 | count() rows", []string{`{"rows":"35"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"10325"}`})

	// Delete all the logs for the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[1],
	}
	check(tenantIDs, "* | count() rows", []string{`{"rows":"3465"}`})
	deleteRows(tenantIDs, "*")
	check(tenantIDs, "* | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"6860"}`})

	// Delete all the logs for the particular day
	filter := "_time:1d offset 2d"
	check(allTenantIDs, filter+" | count() rows", []string{`{"rows":"980"}`})
	deleteRows(allTenantIDs, filter)
	check(allTenantIDs, filter+" | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"5880"}`})

	// Delete logs by _stream filter at the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[0],
	}
	filter = `{host="host-4",app=~"app-.+"}`
	check(tenantIDs, filter+" | count() rows", []string{`{"rows":"588"}`})
	deleteRows(tenantIDs, filter)
	check(tenantIDs, filter+" | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"5292"}`})

	// Delete logs by composite filter at the particular tenant
	tenantIDs = []TenantID{
		allTenantIDs[2],
	}
	filter = `(_msg:3 row_id:23 _time:2d) or (row_id:56 {host=~"host-[23]"} app:*02* tenant_id:~"56")`
	check(tenantIDs, filter+" | count() rows", []string{`{"rows":"8"}`})
	deleteRows(tenantIDs, filter)
	check(tenantIDs, filter+" | count() rows", []string{`{"rows":"0"}`})
	check(allTenantIDs, "* | count() rows", []string{`{"rows":"5284"}`})

	s.MustClose()

	fs.MustRemoveDir(path)
}

func checkQueryResults(t *testing.T, s *Storage, tenantIDs []TenantID, qStr string, hiddenFieldsFilters, resultsExpected []string) {
	t.Helper()

	q, err := ParseQuery(qStr)
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

		for _, c := range db.Columns {
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

	for rowID := 0; rowID < rowsPerDayPerStream; rowID++ {
		for streamID := 0; streamID < streamsPerTenant; streamID++ {
			fields = append(fields[:0], Field{
				Name:  "host",
				Value: fmt.Sprintf("host-%d", streamID),
			}, Field{
				Name:  "app",
				Value: fmt.Sprintf("app-%d", 200+streamID),
			})
			for _, tenantID := range tenantIDs {
				for dayID := int64(0); dayID < days; dayID++ {
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
					lr.MustAdd(tenantID, timestamp, fields, nil)
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

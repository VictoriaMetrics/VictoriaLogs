package logstorage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestStorageSearchStreamIDs(t *testing.T) {
	t.Parallel()

	path := t.Name()
	const partitionName = "foobar"
	s := newTestStorage()
	mustCreateIndexdb(path)
	idb := mustOpenIndexdb(path, partitionName, s)

	tenantID := TenantID{
		AccountID: 123,
		ProjectID: 567,
	}
	getStreamIDForTags := func(tags map[string]string) (streamID, string) {
		st := GetStreamTags()
		for k, v := range tags {
			st.Add(k, v)
		}
		streamTagsCanonical := st.MarshalCanonical(nil)
		PutStreamTags(st)
		id := hash128(streamTagsCanonical)
		sid := streamID{
			tenantID: tenantID,
			id:       id,
		}
		return sid, string(streamTagsCanonical)
	}

	// Create indexdb entries
	const jobsCount = 7
	const instancesCount = 5
	for i := range jobsCount {
		for j := range instancesCount {
			sid, streamTagsCanonical := getStreamIDForTags(map[string]string{
				"job":      fmt.Sprintf("job-%d", i),
				"instance": fmt.Sprintf("instance-%d", j),
			})
			idb.mustRegisterStream(&sid, streamTagsCanonical)
		}
	}
	idb.debugFlush()

	f := func(filterStream string, expectedStreamIDs []streamID) {
		t.Helper()
		sf := mustNewTestStreamFilter(filterStream)
		if expectedStreamIDs == nil {
			expectedStreamIDs = []streamID{}
		}
		sortStreamIDs(expectedStreamIDs)
		for i := range 3 {
			streamIDs := idb.searchStreamIDs([]TenantID{tenantID}, sf, false)
			if !reflect.DeepEqual(streamIDs, expectedStreamIDs) {
				t.Fatalf("unexpected streamIDs on iteration %d; got %v; want %v", i, streamIDs, expectedStreamIDs)
			}
		}
	}

	t.Run("missing-tenant-id", func(t *testing.T) {
		tenantID := TenantID{
			AccountID: 1,
			ProjectID: 2,
		}
		sf := mustNewTestStreamFilter(`{job="job-0",instance="instance-0"}`)
		for i := range 3 {
			streamIDs := idb.searchStreamIDs([]TenantID{tenantID}, sf, false)
			if len(streamIDs) > 0 {
				t.Fatalf("unexpected non-empty streamIDs on iteration %d: %d", i, len(streamIDs))
			}
		}
	})

	// missing-job
	f(`{job="non-existing-job",instance="instance-0"}`, nil)

	// missing-job-re
	f(`{job=~"non-existing-job|",instance="instance-0"}`, nil)

	// missing-job-negative-re
	f(`{job!~"job.+",instance="instance-0"}`, nil)

	// empty-job
	f(`{job="",instance="instance-0"}`, nil)

	// missing-instance
	f(`{job="job-0",instance="non-existing-instance"}`, nil)

	// missing-instance-re
	f(`{job="job-0",instance=~"non-existing-instance|"}`, nil)

	// missing-instance-negative-re
	f(`{job="job-0",instance!~"instance.+"}`, nil)

	// empty-instance
	f(`{job="job-0",instance=""}`, nil)

	// non-existing-tag
	f(`{job="job-0",instance="instance-0",non_existing_tag="foobar"}`, nil)

	// non-existing-non-empty-tag
	f(`{job="job-0",instance="instance-0",non_existing_tag!=""}`, nil)

	// non-existing-tag-re
	f(`{job="job-0",instance="instance-0",non_existing_tag=~"foo.+"}`, nil)

	// non-existing-non-empty-tag-re
	f(`{job="job-0",instance="instance-0",non_existing_tag!~""}`, nil)

	// match-job-instance
	sid, _ := getStreamIDForTags(map[string]string{
		"instance": "instance-0",
		"job":      "job-0",
	})
	f(`{job="job-0",instance="instance-0"}`, []streamID{sid})

	// match-non-existing-tag
	sid, _ = getStreamIDForTags(map[string]string{
		"instance": "instance-0",
		"job":      "job-0",
	})
	f(`{job="job-0",instance="instance-0",non_existing_tag=~"foo|"}`, []streamID{sid})

	// match-job
	var streamIDs []streamID
	for i := range instancesCount {
		sid, _ := getStreamIDForTags(map[string]string{
			"instance": fmt.Sprintf("instance-%d", i),
			"job":      "job-0",
		})
		streamIDs = append(streamIDs, sid)
	}
	f(`{job="job-0"}`, streamIDs)

	// match-instance
	streamIDs = nil
	for i := range jobsCount {
		sid, _ := getStreamIDForTags(map[string]string{
			"instance": "instance-1",
			"job":      fmt.Sprintf("job-%d", i),
		})
		streamIDs = append(streamIDs, sid)
	}
	f(`{instance="instance-1"}`, streamIDs)

	// match-re
	streamIDs = nil
	for _, instanceID := range []int{3, 1} {
		for _, jobID := range []int{0, 2} {
			sid, _ := getStreamIDForTags(map[string]string{
				"instance": fmt.Sprintf("instance-%d", instanceID),
				"job":      fmt.Sprintf("job-%d", jobID),
			})
			streamIDs = append(streamIDs, sid)
		}
	}
	f(`{job=~"job-(0|2)",instance=~"instance-[13]"}`, streamIDs)

	// match-re-empty-match
	streamIDs = nil
	for _, instanceID := range []int{3, 1} {
		for _, jobID := range []int{0, 2} {
			sid, _ := getStreamIDForTags(map[string]string{
				"instance": fmt.Sprintf("instance-%d", instanceID),
				"job":      fmt.Sprintf("job-%d", jobID),
			})
			streamIDs = append(streamIDs, sid)
		}
	}
	f(`{job=~"job-(0|2)|",instance=~"instance-[13]"}`, streamIDs)

	// match-negative-re
	var instanceIDs []int
	for i := range instancesCount {
		if i != 0 && i != 1 {
			instanceIDs = append(instanceIDs, i)
		}
	}
	var jobIDs []int
	for i := range jobsCount {
		if i > 2 {
			jobIDs = append(jobIDs, i)
		}
	}
	streamIDs = nil
	for _, instanceID := range instanceIDs {
		for _, jobID := range jobIDs {
			sid, _ := getStreamIDForTags(map[string]string{
				"instance": fmt.Sprintf("instance-%d", instanceID),
				"job":      fmt.Sprintf("job-%d", jobID),
			})
			streamIDs = append(streamIDs, sid)
		}
	}
	f(`{job!~"job-[0-2]",instance!~"instance-(0|1)"}`, streamIDs)

	// match-negative-re-empty-match
	instanceIDs = nil
	for i := range instancesCount {
		if i != 0 && i != 1 {
			instanceIDs = append(instanceIDs, i)
		}
	}
	jobIDs = nil
	for i := range jobsCount {
		if i > 2 {
			jobIDs = append(jobIDs, i)
		}
	}
	streamIDs = nil
	for _, instanceID := range instanceIDs {
		for _, jobID := range jobIDs {
			sid, _ := getStreamIDForTags(map[string]string{
				"instance": fmt.Sprintf("instance-%d", instanceID),
				"job":      fmt.Sprintf("job-%d", jobID),
			})
			streamIDs = append(streamIDs, sid)
		}
	}
	f(`{job!~"job-[0-2]",instance!~"instance-(0|1)|"}`, streamIDs)

	// match-negative-job
	instanceIDs = []int{2}
	jobIDs = nil
	for i := range jobsCount {
		if i != 1 {
			jobIDs = append(jobIDs, i)
		}
	}
	streamIDs = nil
	for _, instanceID := range instanceIDs {
		for _, jobID := range jobIDs {
			sid, _ := getStreamIDForTags(map[string]string{
				"instance": fmt.Sprintf("instance-%d", instanceID),
				"job":      fmt.Sprintf("job-%d", jobID),
			})
			streamIDs = append(streamIDs, sid)
		}
	}
	f(`{instance="instance-2",job!="job-1"}`, streamIDs)

	mustCloseIndexdb(idb)
	fs.MustRemoveDir(path)

	closeTestStorage(s)
}

// TestSearchStreamIDsSkipCache verifies that searchStreamIDs with skipCache=true bypasses a stale
// stream filter cache, both for reads and writes. This is what lets live tailing pick up streams
// registered after the cache was populated. See https://github.com/VictoriaMetrics/VictoriaLogs/issues/1477
func TestSearchStreamIDsSkipCache(t *testing.T) {
	t.Parallel()

	path := t.Name()
	const partitionName = "foobar"

	s := newTestStorage()
	defer closeTestStorage(s)

	mustCreateIndexdb(path)
	defer fs.MustRemoveDir(path)

	idb := mustOpenIndexdb(path, partitionName, s)
	defer mustCloseIndexdb(idb)

	tenantID := TenantID{AccountID: 12, ProjectID: 34}
	tenantIDs := []TenantID{tenantID}

	registerStream := func(tags map[string]string) streamID {
		st := GetStreamTags()
		for k, v := range tags {
			st.Add(k, v)
		}
		streamTagsCanonical := st.MarshalCanonical(nil)
		PutStreamTags(st)
		sid := streamID{tenantID: tenantID, id: hash128(streamTagsCanonical)}
		idb.mustRegisterStream(&sid, string(streamTagsCanonical))
		return sid
	}

	sidA := registerStream(map[string]string{"job": "job-0"})
	registerStream(map[string]string{"job": "job-1"})
	idb.debugFlush()

	sf := mustNewTestStreamFilter(`{job=~"job-.*"}`)

	// Poison the stream filter cache with a stale result that knows only about stream A,
	// emulating a new stream (job-1) registered after the cache was populated.
	idb.storeStreamIDsToCache(tenantIDs, sf, []streamID{sidA})

	// With the cache enabled the stale entry is returned, so stream job-1 is missing.
	if got := idb.searchStreamIDs(tenantIDs, sf, false); len(got) != 1 {
		t.Fatalf("expected stale cached result with 1 streamID; got %d: %v", len(got), got)
	}

	// With the cache bypassed both streams are resolved straight from the index.
	if got := idb.searchStreamIDs(tenantIDs, sf, true); len(got) != 2 {
		t.Fatalf("expected fresh result with 2 streamIDs; got %d: %v", len(got), got)
	}

	// Bypassing the cache must not populate it, so the stale entry is still served afterwards.
	if got := idb.searchStreamIDs(tenantIDs, sf, false); len(got) != 1 {
		t.Fatalf("expected stale cached result to remain after skipCache search; got %d: %v", len(got), got)
	}
}

func TestGetTenantsIDs(t *testing.T) {
	t.Parallel()

	path := t.Name()
	const partitionName = "foobar"

	s := newTestStorage()
	defer closeTestStorage(s)

	mustCreateIndexdb(path)
	defer fs.MustRemoveDir(path)

	idb := mustOpenIndexdb(path, partitionName, s)
	defer mustCloseIndexdb(idb)

	tenantIDs := []TenantID{
		{AccountID: 0, ProjectID: 0},
		{AccountID: 0, ProjectID: 1},
		{AccountID: 1, ProjectID: 0},
		{AccountID: 1, ProjectID: 1},
		{AccountID: 123, ProjectID: 567},
	}
	getStreamIDForTags := func(tags map[string]string) ([]streamID, string) {
		st := GetStreamTags()
		for k, v := range tags {
			st.Add(k, v)
		}
		streamTagsCanonical := st.MarshalCanonical(nil)
		PutStreamTags(st)
		id := hash128(streamTagsCanonical)
		sids := make([]streamID, 0, len(tenantIDs))
		for _, tenantID := range tenantIDs {
			sid := streamID{
				tenantID: tenantID,
				id:       id,
			}

			sids = append(sids, sid)
		}

		return sids, string(streamTagsCanonical)
	}

	// Create indexdb entries
	const jobsCount = 7
	const instancesCount = 5
	for i := range jobsCount {
		for j := range instancesCount {
			sids, streamTagsCanonical := getStreamIDForTags(map[string]string{
				"job":      fmt.Sprintf("job-%d", i),
				"instance": fmt.Sprintf("instance-%d", j),
			})
			for _, sid := range sids {
				idb.mustRegisterStream(&sid, streamTagsCanonical)
			}

		}
	}
	idb.debugFlush()

	// run the test
	result := idb.searchTenants()
	if !reflect.DeepEqual(result, tenantIDs) {
		t.Fatalf("unexpected tensntIds; got %v; want %v", result, tenantIDs)
	}
}

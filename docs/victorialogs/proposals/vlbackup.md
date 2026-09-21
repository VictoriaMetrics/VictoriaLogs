# VictoriaLogs Backup and Restore Tooling

Author: YurDuiachenko

## Background

Currently, there is no dedicated VictoriaLogs backup and restore tooling.

VictoriaMetrics already provides `vmbackup` and `vmrestore`, but these tools are designed around the VictoriaMetrics storage lifecycle and don't support VictoriaLogs-specific per-day partition snapshot and restore operations.

VictoriaLogs documentation currently describes the backup process as a sequence of manual steps: create a partition snapshot, copy it to external storage with `rsync` or `rclone`, and delete the snapshot afterward.

This is a simple and flexible workflow, but it has several drawbacks:

- backup and restore operations require the operator to manually coordinate VictoriaLogs partition management APIs with `rsync` or `rclone`, including snapshot creation, data transfer, cleanup, and restore;
- the documented workflow maintains the latest state of each partition in a backup destination, but doesn't provide a native way to create a single recovery point containing states of multiple partitions;
- if multiple independent point-in-time recovery points are required, the operator must additionally manage their metadata and storage layout externally. Reusing immutable partition data between such recovery points also requires additional storage-specific orchestration, such as server-side copies or deduplication.

## High-level proposal

The proposal is to introduce two VictoriaLogs-specific binaries:

- `vlbackup` - creates full and selective backups of VictoriaLogs;
- `vlrestore` - restores VictoriaLogs from backups.

They will manage the VictoriaLogs-specific snapshot lifecycle and add a specific layout on top of the existing backup destination to support point-in-time recovery,
reuse unchanged partition data without requiring server-side copies, and preserve historical partitions independently of VictoriaLogs retention.

Both tools will reuse the existing backup and restore libraries where possible instead of introducing a new backup engine or adding VictoriaLogs-specific logic to `vmbackup` and `vmrestore`.

A `vlbackupmanager` can be introduced in another proposal to provide scheduling and retention on top of `vlbackup` and `vlrestore`.

## Goals

- Provide dedicated `vlbackup` and `vlrestore` tools for VictoriaLogs.
- Support full-storage and selective partition backup and restore.
- Support independent recovery points while reusing unchanged partition data without server-side copies.
- Handle VictoriaLogs partition snapshot lifecycle and partition-set capture automatically.

## Non-Goals

- Implementing a new backup or remote storage engine.
- Changing the VictoriaLogs storage format or partition layout.
- Implementing scheduling, retention policies, or automated recovery-point lifecycle management.
- Implementing cluster-wide orchestration.

---

## Detailed design

There are the following popular ways to make backups:

- Point in time backups for all the data currently stored at VictoriaLogs. Such backups are good for recovery of all the data seen during the backup moment (aka point in time recovery).
- Historical archives - to store data for historical days at the backup storage, so it could be restored and investigated if needed, even if this data is dropped at VictoriaLogs because of the configured retention.

Proposed binaries will provide functionality for both use cases due to the specific backup destination hierarchy: data is stored only once and then referenced by recovery-point metadata.
That allows making very fast and cheap incremental backups, while they still can be properly restored.

Backup destination hierarchy looks like this:

```text
partitions/
  <partition>/
    data/
      datadb/
        <part-id>/...
      indexdb/
        <part-id>/...
    states/
      <state-id>/
        datadb/
          parts.json
        indexdb/
          parts.json

recovery-points/
  <recovery-point>.json
```

A recovery point contains a set of daily partition states.

Each partition has its own shared physical data pool under `partitions/<partition>/data/`.

The snapshot-specific metadata `datadb/parts.json` and `indexdb/parts.json` files are stored under an immutable `states/<state-id>/` directory.

`<state-id>` identifies the contents of these state files. Therefore, identical partition states may be shared by multiple recovery points, while a change in `parts.json` creates a new state.

A recovery-point manifest maps every partition included in the backup to the corresponding state:

```json
{
   "version": 1,
   "id": "20260830T100000Z",
   "scope": "full",
   "partitions": {
      "20260828": "<state-id-1>",
      "20260829": "<state-id-2>",
      "20260830": "<state-id-3>"
   }
}
```

### vlbackup

The interface is intentionally similar to `vmbackup`:

```bash
./vlbackup \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -dst=s3://<bucket>/<path/to/backup>
```

`-partitionManage.url` specifies the VictoriaLogs partition management API URL used for operations such as listing partitions and creating or deleting snapshots.

`-partitionManage.authKey` can be used when the partition management API is protected with `-partitionManageAuthKey`.

When `-partition` isn't specified, `vlbackup`:

1. Creates snapshots for all active VictoriaLogs partitions in a single partition snapshot API request.
2. For each partition snapshot:
   - uploads physical storage parts which aren't already present under `partitions/<partition>/data/`;
   - stores snapshot metadata under `partitions/<partition>/states/<state-id>/`;
   - adds `<partition>:<state-id>` records to the recovery-point manifest being built locally;
   - deletes the partition snapshot immediately after its data and state have been stored successfully.
3. Writes `recovery-points/<recovery-point>.json` last as the commit record.

If the backup is interrupted before the recovery-point manifest is written, the recovery point isn't available for restore.

An interrupted backup may leave unreferenced physical data or partition states in the backup destination. Their cleanup is outside the scope of `vlbackup` and can be handled by `vlbackupmanager`.

#### Partition selection

A particular partition can be selected with `-partition`:

```bash
./vlbackup \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -partition=20260828 \
  -dst=s3://<bucket>/<path/to/backup>
```

In this case, the same recovery point creation workflow is performed as for full storage backup, but only for one partition and with `"scope": "partition"`.

### vlrestore

A full recovery point can be restored with:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -recoveryPoint=20260830T100000Z \
  -storageDataPath=</path/to/victoria-logs-data>
```

And it must be performed while VictoriaLogs is stopped.

When `-partition` isn't specified, `vlrestore`:

1. Reads `recovery-points/<recovery-point>.json`, verifies that its `scope` is `full`, and gets the partition list and their states.
2. For each partition in the recovery point:
   - loads the corresponding `datadb/parts.json` and `indexdb/parts.json` from `partitions/<partition>/states/<state-id>/`;
   - resolves the physical parts referenced by this state under `partitions/<partition>/data/`;
   - restores the partition into a temporary directory using the shared restore library;
   - writes the snapshot-specific `parts.json` files into the restored partition.
3. If restoring any partition fails, removes the temporary restored data and aborts without modifying local partitions. 
4. After all partitions have been restored successfully into temporary directories, replaces the corresponding local partition data one by one.
5. If replacement fails after some partitions have already been replaced, those partitions remain restored to the selected recovery point. Re-running `vlrestore` with the same recovery point retries the operation.
6. After all partitions referenced by the recovery point have been replaced successfully, removes local partitions which aren't present in the selected full recovery point.

A recovery point with `scope: "partition"` can't be used for a full-storage restore.

#### Selective partition restore

A partition can be restored from a specific recovery point:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -recoveryPoint=20260830T100000Z \
  -partition=20260828 \
  -storageDataPath=</path/to/victoria-logs-data> \
  -partitionManage.url=http://localhost:9428/internal/partition
```

`-partitionManage.url` is used for partition detach and attach operations. `-partitionManage.authKey` can be used when the partition management API is protected with `-partitionManageAuthKey`.

In this case, `vlrestore` takes the partition state from the selected recovery-point manifest and restores only that partition. The selected recovery point may have either `scope: "full"` or `scope: "partition"` as long as it contains the selected partition.

It can be performed on a running VictoriaLogs instance by detaching the selected local partition if it exists, replacing its data, and attaching the restored partition afterward.

A partition can also be restored without explicitly selecting a recovery point:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -partition=20260828 \
  -storageDataPath=</path/to/victoria-logs-data> \
  -partitionManage.url=http://localhost:9428/internal/partition
```

When `-recoveryPoint` isn't specified, `vlrestore` considers only committed recovery points with `scope: "partition"` which contain the selected partition and restores it from the newest matching recovery point.

Recovery points with `scope: "full"` aren't considered for implicit partition restore.

Available recovery points can be listed with:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -list
```

### Backups on VLCluster

In a VLCluster, `vlbackup` should be run independently for every `vlstorage` node, with a separate destination path for each node.

For example:

```bash
vlstorage-1$ ./vlbackup -partitionManage.url=http://vlstorage-1:9491/internal/partition -dst=s3://<bucket>/vlstorage-1
vlstorage-2$ ./vlbackup -partitionManage.url=http://vlstorage-2:9491/internal/partition -dst=s3://<bucket>/vlstorage-2
vlstorage-3$ ./vlbackup -partitionManage.url=http://vlstorage-3:9491/internal/partition -dst=s3://<bucket>/vlstorage-3
```

---

## Metrics

`vlbackup` and `vlrestore` expose Prometheus-compatible metrics through the standard `/metrics` endpoint.

The proposed default listen addresses are:

- `vlbackup`: `:9420`;
- `vlrestore`: `:9421`.

The listen address can be configured with `-httpListenAddr`.

Metrics provided by the shared backup and restore libraries, such as `vm_backups_uploaded_bytes_total` and `vm_backups_downloaded_bytes_total`, are reused directly.

VictoriaLogs-specific metrics should cover:

- successfully processed and failed partitions and recovery points;
- backup and restore errors;
- backup and restore duration.

## Testing

### Unit tests

Unit tests should cover VictoriaLogs-specific logic introduced by `vlbackup` and `vlrestore`, including:

- partition discovery, selection, snapshot mapping, and destination mapping;
- VictoriaLogs snapshot and partition management API handling, including snapshot cleanup;
- recovery-point manifest handling.

Generic backup and restore behavior is covered by the existing shared-library tests.

### Application tests

Application tests should run VictoriaLogs with temporary storage and cover:

- full and selective partition backup and restore;
- incremental backup with physical data reuse and creation of multiple recovery points for the same partition;
- restoring an older recovery point after a newer partition state has already been backed up;
- failed or incomplete multi-partition backups without exposing an incomplete recovery point;
- full restore failure during staging and retry after a partial replacement.

Performance and resource usage should also be compared with the existing `rclone`-based workflow under ingestion and query load.

## Documentation

This proposal can serve as the basis for the `vlbackup` and `vlrestore` documentation, covering CLI usage, backup and restore workflows, metrics, and VLCluster usage.

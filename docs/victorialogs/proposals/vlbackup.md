# VictoriaLogs Backup and Restore Tooling

Author: YurDuiachenko

## Background

Currently, there is no dedicated VictoriaLogs backup and restore tooling:

- VictoriaMetrics already provides `vmbackup` and `vmrestore`, but these tools are designed around the VictoriaMetrics storage lifecycle and don't support VictoriaLogs-specific partition snapshot and restore operations.
- The VictoriaLogs documentation currently describes the backup process as a sequence of manual steps: create a partition snapshot, copy it to external storage with `rsync` or `rclone`, and delete the snapshot afterward.

## High-level proposal

The proposal is to introduce two VictoriaLogs-specific binaries:

- `vlbackup` - creates full and incremental backups of VictoriaLogs;
- `vlrestore` - restores VictoriaLogs from backups.

Both tools reuse the existing backup and restore libraries where possible instead of introducing a new backup engine or adding VictoriaLogs-specific logic to `vmbackup` and `vmrestore`.

A `vlbackupmanager` may be introduced later to provide scheduling, retention, and recovery-point management on top of `vlbackup` and `vlrestore`.

## Goals

- Provide dedicated `vlbackup` and `vlrestore` tools for VictoriaLogs.
- Support full-storage and selective partition backup and restore.
- Support incremental backups and independent recovery points using `origin`.
- Handle VictoriaLogs partition snapshot lifecycle and partition-set synchronization automatically.

## Non-Goals

- Implementing a new backup or remote storage engine.
- Changing the VictoriaLogs storage format or partition layout.
- Implementing scheduling, retention, or cluster-wide orchestration in the initial implementation.

## Detailed design

### vlbackup

The interface is intentionally similar to `vmbackup`:

```bash
./vlbackup \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -dst=s3://<bucket>/<path/to/backup>
```

`-partitionManage.url` specifies the VictoriaLogs partition management API URL used for operations such as listing partitions and creating or deleting snapshots.

`-partitionManage.authKey` can be used when the partition management API is protected with `-partitionManageAuthKey`.

When `-partition` isn't set, `vlbackup` discovers all available VictoriaLogs partitions and backs each partition up independently under `<dst>/<YYYYMMDD>`.
For example, partition `20260828` is stored under `s3://<bucket>/<path/to/backup>/20260828`.

For each partition, `vlbackup` creates a VictoriaLogs snapshot, passes it to the shared backup library, and attempts to delete the snapshot after the backup finishes or fails.
As with `vmbackup`, a new partition destination results in a full backup, while an existing one is updated incrementally.

#### Partition selection

A particular partition can be selected with `-partition`:

```bash
./vlbackup \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -partition=20260828 \
  -dst=s3://<bucket>/<path/to/backup>
```

In this case, only `<dst>/20260828` is created or updated (other partitions aren't touched) and partition reconciliation isn't performed.

#### Partition reconciliation

When `-partition` isn't set, `vlbackup` also synchronizes the set of destination partitions with the partitions currently present in VictoriaLogs.

For example, if the destination contains partitions `[20260801, 20260802, 20260803, 20260804]`,
while VictoriaLogs currently contains only `[20260801, 20260803, 20260804]`, then `20260802` is removed from the destination after the remaining partitions have been backed up successfully.

This follows the same synchronization semantics as `vmbackup`, but at the partition level.

The operation is performed in the following order:

1. Mark the backup as incomplete.
2. Discover source and destination partitions.
3. Back up all current partitions.
4. Remove stale destination partitions if all partition backups succeeded.
5. Mark the backup as complete.

If a backup fails before reconciliation completes, the destination remains incomplete and can be retried.
For backups without `-partition`, `vlbackup` keeps an overall completion state in addition to the per-partition completion state provided by the shared backup library.

#### Origin backup

Updating an existing destination provides an incremental backup but doesn't preserve its previous state as an independent recovery point.

A new destination can use an existing backup as `origin` to create an independent recovery point without re-uploading unchanged data:

```bash
./vlbackup \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -dst=s3://<bucket>/<path/to/new/backup> \
  -origin=s3://<bucket>/<path/to/existing/backup>
```

Origin reuse is performed between corresponding partitions. For example, when backing up partition `20260828`, `vlbackup` uses `<origin>/20260828` as the origin for `<dst>/20260828`.

Only partitions currently present in VictoriaLogs are created in the new destination. Within each partition, unchanged data can be reused server-side from `origin`, while new or changed data is uploaded.

When `-partition` is specified, the corresponding `<origin>/<partition>` is used for `<dst>/<partition>`.

The origin isn't modified.

### vlrestore

The interface is similar to `vmrestore`:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -storageDataPath=</path/to/victoria-logs-data> \
  -partitionManage.url=http://localhost:9428/internal/partition
```

`-partitionManage.url` is used for partition detach and attach operations. `-partitionManage.authKey` can be used when the partition management API is protected with `-partitionManageAuthKey`.

Before a full restore, `vlrestore` verifies that the source represents a completed backup and discovers partition backups directly from `-src`.

When `-partition` isn't set, the local VictoriaLogs partitions are synchronized with the backup. For example, if the backup contains `[20260801, 20260803, 20260804]`, while the local storage additionally contains `20260802`, then `20260802` is removed after the restore completes.

This follows the synchronization model of `vmrestore`: the local partition set is synchronized with the backup rather than merged with it.

Partitions are restored independently:

1. Restore a partition into a temporary directory and validate it.
2. Detach the corresponding local partition if it exists.
3. Replace the partition data.
4. Attach the restored partition.
5. Continue with the next partition.

Local partitions which aren't present in the backup are detached and removed only after all backup partitions have been restored successfully.

A particular partition can be restored with `-partition`. The selected partition backup must be complete:

```bash
./vlrestore \
  -src=s3://<bucket>/<path/to/backup> \
  -storageDataPath=</path/to/victoria-logs-data> \
  -partitionManage.url=http://localhost:9428/internal/partition \
  -partition=20260828
```

In this case, the backup is read from `<src>/20260828`, and only that partition is restored and replaced.

### Backups on VLCluster

In a VLCluster, `vlbackup` should be run independently for every `vlstorage` node, with a separate destination path for each node.

For example:

```bash
vlstorage-1$ ./vlbackup -partitionManage.url=http://vlstorage-1:9491/internal/partition -dst=s3://<bucket>/vlstorage-1
vlstorage-2$ ./vlbackup -partitionManage.url=http://vlstorage-2:9491/internal/partition -dst=s3://<bucket>/vlstorage-2
vlstorage-3$ ./vlbackup -partitionManage.url=http://vlstorage-3:9491/internal/partition -dst=s3://<bucket>/vlstorage-3
```

### Metrics

`vlbackup` and `vlrestore` expose Prometheus-compatible metrics through the standard `/metrics` endpoint.

The proposed default listen addresses are:

- `vlbackup`: `:9420`;
- `vlrestore`: `:9421`.

The listen address can be configured with `-httpListenAddr`.

Metrics provided by the shared backup and restore libraries, such as `vm_backups_uploaded_bytes_total` and `vm_backups_downloaded_bytes_total`, are reused directly.

VictoriaLogs-specific metrics should cover:

- successfully processed and failed partitions;
- backup and restore errors;
- stale partitions removed during reconciliation;
- backup and restore duration.

## Testing

### Unit tests

Unit tests should cover VictoriaLogs-specific logic introduced by `vlbackup` and `vlrestore`, including:

- partition discovery, selection, and destination mapping;
- VictoriaLogs snapshot and partition management API handling;
- partition reconciliation and overall backup completion state.

Generic backup and restore behavior is covered by the existing shared-library tests.

### Application tests

Application tests should run VictoriaLogs with temporary storage and cover:

- full and selective partition backup and restore;
- incremental backup and recovery-point creation with `origin`;
- partition reconciliation when the source partition set changes;
- failed or incomplete multi-partition backups.

Performance and resource usage should also be compared with the existing `rclone`-based workflow under ingestion and query load.

## Documentation

This proposal can serve as the basis for the `vlbackup` and `vlrestore` documentation, covering CLI usage, backup and restore workflows, `origin`, metrics, and VLCluster usage.

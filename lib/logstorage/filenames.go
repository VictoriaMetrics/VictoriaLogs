package logstorage

const (
	columnNamesFilename        = "column_names.bin"
	columnIdxsFilename         = "column_idxs.bin"
	metaindexFilename          = "metaindex.bin"
	indexFilename              = "index.bin"
	columnsHeaderIndexFilename = "columns_header_index.bin"
	columnsHeaderFilename      = "columns_header.bin"
	timestampsFilename         = "timestamps.bin"
	oldValuesFilename          = "field_values.bin"
	oldBloomFilename           = "field_bloom.bin"
	valuesFilename             = "values.bin"
	bloomFilename              = "bloom.bin"
	messageValuesFilename      = "message_values.bin"
	messageBloomFilename       = "message_bloom.bin"

	// Filename for per-row marker data (e.g. delete markers).
	deleteMarkerFilename = "deleted_rows.bin"

	metadataFilename = "metadata.json"
	partsFilename    = "parts.json"

	// Filename for delete tasks storage at partition level.
	deleteTasksFilename = "delete_tasks.json"

	indexdbDirname    = "indexdb"
	datadbDirname     = "datadb"
	partitionsDirname = "partitions"
	snapshotsDirname  = "snapshots"
)

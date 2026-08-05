package fs

// MustReaderAtOpenerTask task to open ReaderAt files in parallel.
type MustReaderAtOpenerTask struct {
	path     string
	rc       *MustReadAtCloser
	fileSize *uint64
}

// NewMustReaderAtOpenerTask creates new task for writing the data from src to the path
//
// ParallelMustReaderAtOpener speeds up opening multiple ReaderAt files on high-latency
// storage systems such as NFS or Ceph.
func NewMustReaderAtOpenerTask(path string, rc *MustReadAtCloser, fileSize *uint64) *MustReaderAtOpenerTask {
	return &MustReaderAtOpenerTask{
		path:     path,
		rc:       rc,
		fileSize: fileSize,
	}
}

func (t *MustReaderAtOpenerTask) Run() {
	*t.rc = MustOpenReaderAt(t.path)
	*t.fileSize = MustFileSize(t.path)
}

// MustCloser must implement MustClose() function.
type MustCloser interface {
	MustClose()
}

// MustCloserTask task to close all the MustCloser in parallel.
//
// Parallel closing reduces the time needed to flush the data to the underlying files on close
// on high-latency storage systems such as NFS or Ceph.
type MustCloserTask struct {
	c MustCloser
}

// NewMustCloserTask creates new task for writing the data from src to the path
//
// NewMustCloserTask speeds up opening multiple MustCloser files on high-latency
// storage systems such as NFS or Ceph.
func NewMustCloserTask(c MustCloser) *MustCloserTask {
	return &MustCloserTask{
		c: c,
	}
}

func (t *MustCloserTask) Run() {
	t.c.MustClose()
}

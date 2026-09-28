package durablefile

import "os"

// LineSpan is a byte range [Start, End) within a buffer, spanning one line
// including its terminating newline when present.
type LineSpan struct {
	Start int64
	End   int64
}

// FsyncDir fsyncs a directory so renames into it are durable.
func FsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

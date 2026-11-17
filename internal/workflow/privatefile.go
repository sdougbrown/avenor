package workflow

import "os"

// WritePrivateFile writes one file owner-only. os.WriteFile keeps an existing
// file's mode, so the file is chmodded too: a file an earlier version wrote
// world-readable becomes owner-only the next time this version rewrites it.
func WritePrivateFile(path string, content []byte) error {
	if err := os.WriteFile(path, content, StateFilePerm); err != nil {
		return err
	}
	return os.Chmod(path, StateFilePerm)
}

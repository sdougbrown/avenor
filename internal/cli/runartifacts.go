package cli

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// writeResultFile writes text to path atomically, so a reader that waits for
// the completion sentinel never sees a partial answer.
func writeResultFile(path, text string) error {
	return writeFileAtomic(path, ".result-*.tmp", text)
}

// writePIDFile publishes the current process's pid so a caller can track the
// run without inferring a pid from a launcher shell.
func writePIDFile(path string) error {
	return writeFileAtomic(path, ".pid-*.tmp", strconv.Itoa(os.Getpid())+"\n")
}

// removePIDFile removes path only while it still holds pid, so a later run
// that reused the path keeps its own file.
func removePIDFile(path string, pid int) {
	raw, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(raw)) != strconv.Itoa(pid) {
		return
	}
	_ = os.Remove(path)
}

func writeFileAtomic(path, pattern, text string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.WriteString(text); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

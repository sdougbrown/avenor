package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// atomicWriteHook, when non-nil, observes each successful atomic write by
// destination path. Tests use it to pin write order.
var atomicWriteHook func(string)

// pidFileRemoveCheck, when non-nil, runs after removePIDFile has read and
// verified the file and before it re-checks the path. Tests use it to
// interleave a concurrent run that replaced the pid file.
var pidFileRemoveCheck func()

// validateArtifactPaths rejects artifact paths a line-oriented consumer cannot
// name: a newline in the result or pid path would split the value, and two
// flags naming one path would let the last write clobber the answer.
func validateArtifactPaths(resultFile, pidFile, sentinelFile string) error {
	flags := []struct {
		name       string
		path       string
		singleLine bool
	}{
		{"--result-file", resultFile, true},
		{"--pid-file", pidFile, true},
		{"--sentinel-file", sentinelFile, false},
	}
	seen := make(map[string]string, len(flags))
	for _, f := range flags {
		if f.path == "" {
			continue
		}
		if f.singleLine && strings.ContainsAny(f.path, "\r\n") {
			return fmt.Errorf("%s must be a single-line path", f.name)
		}
		clean := filepath.Clean(f.path)
		if other, ok := seen[clean]; ok {
			return fmt.Errorf("%s and %s name the same path %q", other, f.name, clean)
		}
		seen[clean] = f.name
	}
	return nil
}

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
// that reused the path keeps its own file. Content and inode are verified
// through the open handle and re-checked against the path after the read; a
// rename into path between that check and os.Remove still loses the race,
// which is the residual window.
func removePIDFile(path string, pid int) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil {
		return
	}
	raw, err := io.ReadAll(f)
	if err != nil || strings.TrimSpace(string(raw)) != strconv.Itoa(pid) {
		return
	}
	if pidFileRemoveCheck != nil {
		pidFileRemoveCheck()
	}
	current, err := os.Stat(path)
	if err != nil || !os.SameFile(opened, current) {
		return
	}
	_ = os.Remove(path)
}

// writeFileAtomic writes text to path through a sibling tmp file, so a reader
// never observes a partial file.
func writeFileAtomic(path, pattern, text string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.WriteString(text); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}
	if atomicWriteHook != nil {
		atomicWriteHook(path)
	}
	return nil
}

package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sdougbrown/avenor/internal/durablefile"
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
// flags naming one path would let the last write clobber the answer or the pid
// contract. Paths are compared as absolutes with any existing parent directory
// resolved, so neither a relative spelling nor a symlinked directory can slip
// past another flag's path, and existing files are also compared by identity.
func validateArtifactPaths(resultFile, pidFile, sentinelFile, onEvent string) error {
	flags := []struct {
		name       string
		path       string
		singleLine bool
	}{
		{"--result-file", resultFile, true},
		{"--pid-file", pidFile, true},
		{"--sentinel-file", sentinelFile, false},
		{"--on-event", onEvent, false},
	}
	type seenPath struct {
		name string
		abs  string
		info os.FileInfo
	}
	seen := make([]seenPath, 0, len(flags))
	for _, f := range flags {
		if f.path == "" {
			continue
		}
		if f.singleLine && strings.ContainsAny(f.path, "\r\n") {
			return fmt.Errorf("%s must be a single-line path", f.name)
		}
		abs, err := filepath.Abs(f.path)
		if err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
		// A symlinked directory can hide that two spellings reach one file, so
		// compare through the resolved parent when it exists. The file itself is
		// opened without following a final symlink; see NewEventWriter.
		if parent, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
			abs = filepath.Join(parent, filepath.Base(abs))
		}
		info, err := os.Stat(abs)
		if err != nil {
			info = nil
		}
		for _, other := range seen {
			same := other.abs == abs
			if !same && info != nil && other.info != nil {
				same = os.SameFile(info, other.info)
			}
			if same {
				return fmt.Errorf("%s and %s name the same path %q", other.name, f.name, abs)
			}
		}
		seen = append(seen, seenPath{name: f.name, abs: abs, info: info})
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
// never observes a partial file, and reports the completed write to
// atomicWriteHook.
func writeFileAtomic(path, pattern, text string) error {
	if err := durablefile.WriteFileAtomic(path, pattern, []byte(text), 0o600); err != nil {
		return err
	}
	if atomicWriteHook != nil {
		atomicWriteHook(path)
	}
	return nil
}

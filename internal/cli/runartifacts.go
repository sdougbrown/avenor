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
// While the file permission handler is active, the request and response files it
// derives from the sentinel join the set: the handler writes them over whatever
// a flag left at those paths.
func validateArtifactPaths(resultFile, pidFile, sentinelFile, onEvent, permissionHandler string, autoApprove bool) error {
	type artifact struct {
		name       string
		path       string
		singleLine bool
	}
	artifacts := []artifact{
		{"--result-file", resultFile, true},
		{"--pid-file", pidFile, true},
		{"--sentinel-file", sentinelFile, false},
		{"--on-event", onEvent, false},
	}
	if sentinelFile != "" && permissionHandler == "" && !autoApprove {
		base := DerivePermBase(sentinelFile)
		artifacts = append(artifacts,
			artifact{"the permission request file", base + ".req", false},
			artifact{"the permission response file", base + ".req.response", false},
		)
	}
	type seenPath struct {
		name string
		abs  string
		info os.FileInfo
	}
	seen := make([]seenPath, 0, len(artifacts))
	for _, f := range artifacts {
		if f.path == "" {
			continue
		}
		if f.singleLine && strings.ContainsAny(f.path, "\r\n") {
			return fmt.Errorf("%s must be a single-line path", f.name)
		}
		abs, err := ResolveArtifactPath(f.path)
		if err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
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

// publishPIDFile writes this run's pid to path and returns the cleanup that
// removes it on exit. Each run mode publishes only after its own setup that can
// fail has passed, so a run rejected during setup never writes or removes a pid
// file another run owns.
func publishPIDFile(path string, stderr io.Writer) (func(), bool) {
	if path == "" {
		return func() {}, true
	}
	if err := writePIDFile(path); err != nil {
		fmt.Fprintf(stderr, "avenor: pid-file: %v\n", err)
		return nil, false
	}
	return func() { removePIDFile(path, os.Getpid()) }, true
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

// ResolveArtifactPath names an artifact the way this package compares them:
// absolute, with any existing parent directory resolved through symlinks, so
// two spellings that reach one file through a symlinked directory compare equal
// even before the file exists. The final element is never resolved, matching
// how an artifact file is opened without following a final symlink; see
// NewEventWriter.
//
// Callers that compare artifact paths, including warm-turn admission in
// internal/stable, use this so one symlink rule covers every writer.
func ResolveArtifactPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if parent, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
		return filepath.Join(parent, filepath.Base(abs)), nil
	}
	return abs, nil
}

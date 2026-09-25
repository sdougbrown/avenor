//go:build unix

package workflowcontroller

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

// fakeFileInfo is an os.FileInfo over a caller-supplied mode and Stat_t, so
// mode-bit rules can be tested without depending on OS-specific lstat output
// for real symlinks (Linux reports 0777, macOS 0755).
type fakeFileInfo struct {
	mode os.FileMode
	st   *syscall.Stat_t
}

func (f fakeFileInfo) Name() string       { return "fake" }
func (f fakeFileInfo) Size() int64        { return 0 }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return false }
func (f fakeFileInfo) Sys() any           { return f.st }

// TestCheckOwnedModeIgnoresSymlinkModeBits proves a symlink entry passes the
// write-mode check regardless of its permission bits — the kernel ignores
// them when resolving a path, and Linux lstat always reports 0777 — while a
// regular file with the same bits is still rejected. Ownership is enforced
// for both.
func TestCheckOwnedModeIgnoresSymlinkModeBits(t *testing.T) {
	owned := &syscall.Stat_t{Uid: uint32(os.Getuid())}

	if err := checkOwnedMode(fakeFileInfo{mode: os.ModeSymlink | 0o777, st: owned}, "/x/link"); err != nil {
		t.Fatalf("symlink with 0777 bits: %v, want the mode bits ignored", err)
	}
	if err := checkOwnedMode(fakeFileInfo{mode: 0o777, st: owned}, "/x/file"); !errors.Is(err, ErrAdapterUntrusted) {
		t.Fatalf("regular file with 0777 bits: %v, want ErrAdapterUntrusted", err)
	}
	foreign := &syscall.Stat_t{Uid: uint32(os.Getuid()) + 1}
	if err := checkOwnedMode(fakeFileInfo{mode: os.ModeSymlink | 0o755, st: foreign}, "/x/link"); !errors.Is(err, ErrAdapterUntrusted) {
		t.Fatalf("foreign-owned symlink: %v, want ErrAdapterUntrusted", err)
	}
}

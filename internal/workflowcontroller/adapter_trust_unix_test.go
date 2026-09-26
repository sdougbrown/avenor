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

// TestCheckOwnedModeStickyDirectory proves the sticky-bit exception: a
// world-writable directory is tolerated only when it also carries the sticky
// bit (mode 1777, e.g. /tmp), which prevents other users from removing or
// replacing entries they do not own. A world-writable directory without the
// sticky bit, and a world-writable regular file even with the sticky bit,
// are both rejected — the exception excuses directories only.
func TestCheckOwnedModeStickyDirectory(t *testing.T) {
	owned := &syscall.Stat_t{Uid: uint32(os.Getuid())}

	tests := []struct {
		name    string
		mode    os.FileMode
		wantErr bool
	}{
		{
			name:    "world-writable directory with sticky bit is accepted",
			mode:    os.ModeDir | 0o777 | os.ModeSticky,
			wantErr: false,
		},
		{
			name:    "world-writable directory without sticky bit is rejected",
			mode:    os.ModeDir | 0o777,
			wantErr: true,
		},
		{
			name:    "world-writable regular file with sticky bit is rejected",
			mode:    0o777 | os.ModeSticky,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkOwnedMode(fakeFileInfo{mode: tt.mode, st: owned}, "/x")
			if tt.wantErr {
				if !errors.Is(err, ErrAdapterUntrusted) {
					t.Fatalf("got %v, want ErrAdapterUntrusted", err)
				}
			} else if err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

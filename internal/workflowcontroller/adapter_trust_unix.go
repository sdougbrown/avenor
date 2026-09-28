//go:build unix

package workflowcontroller

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// securePath verifies that path and every parent directory up to / are owned
// by the current process UID or root, and that none of them (including path
// itself) are group- or world-writable. Symlinked path components are checked
// as they appear, without resolution: a symlink's permission bits are
// meaningless (the kernel ignores them when walking the path) and vary by OS
// (Linux lstat always reports 0777), so only their ownership is enforced.
func securePath(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	cur := abs
	for {
		fi, err := os.Lstat(cur)
		if err != nil {
			return err
		}
		if err := checkOwnedMode(fi, cur); err != nil {
			return err
		}
		if cur == string(filepath.Separator) {
			return nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return nil
		}
		cur = parent
	}
}

// checkOwnedMode enforces the ownership and write-mode rules for one path
// element. A world-writable directory is tolerated only when it carries the
// sticky bit (mode 1777, e.g. /tmp): the sticky bit prevents other users from
// removing or replacing entries they do not own. Symlinks are exempt from the
// write-mode check — the kernel ignores a symlink's permission bits when
// resolving a path — but their ownership is still enforced.
func checkOwnedMode(fi os.FileInfo, path string) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%w: cannot read ownership of %s", ErrAdapterUntrusted, path)
	}
	uid := uint32(os.Getuid())
	if st.Uid != uid && st.Uid != 0 {
		return fmt.Errorf("%w: %s is owned by uid %d, not the current uid or root", ErrAdapterUntrusted, path, st.Uid)
	}
	mode := fi.Mode()
	if mode&os.ModeSymlink != 0 {
		return nil
	}
	if mode&(0o020|0o002) != 0 && !(mode&0o002 != 0 && mode&os.ModeSticky != 0 && mode.IsDir()) {
		return fmt.Errorf("%w: %s is group- or world-writable (%o)", ErrAdapterUntrusted, path, mode.Perm())
	}
	return nil
}

// execIdentity is the full stat identity of a resolved executable, recorded
// at load time and rechecked before every execution. Size, mtime, and ctime
// accompany device and inode because Linux filesystems (ext4, tmpfs) reuse
// inode numbers: deleting an executable and writing a new one can produce the
// same device and inode, and only a metadata or content difference exposes the
// replacement.
type execIdentity struct {
	dev     uint64
	ino     uint64
	size    int64
	mtimeNS int64
	ctimeNS int64
}

// resolveSecureExecutable validates a manifest executable and records its
// resolved identity. The path must be absolute, resolve through symlinks to a
// regular file executable by its owner, and every directory from it up to /
// plus the resolved file itself must pass the ownership/mode checks.
func resolveSecureExecutable(executable string) (resolved string, id execIdentity, err error) {
	if !filepath.IsAbs(executable) {
		return "", execIdentity{}, fmt.Errorf("executable %q must be an absolute path", executable)
	}
	if err := securePath(executable); err != nil {
		return "", execIdentity{}, err
	}
	resolved, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return "", execIdentity{}, err
	}
	if resolved != executable {
		if err := securePath(resolved); err != nil {
			return "", execIdentity{}, err
		}
	}
	fi, err := os.Lstat(resolved)
	if err != nil {
		return "", execIdentity{}, err
	}
	if !fi.Mode().IsRegular() {
		return "", execIdentity{}, fmt.Errorf("%w: %s is not a regular file", ErrAdapterUntrusted, resolved)
	}
	if fi.Mode()&0o100 == 0 {
		return "", execIdentity{}, fmt.Errorf("%w: %s is not executable by its owner", ErrAdapterUntrusted, resolved)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", execIdentity{}, fmt.Errorf("%w: cannot read file identity of %s", ErrAdapterUntrusted, resolved)
	}
	mtimeNS, ctimeNS := statTimestamps(fi, st)
	return resolved, execIdentity{
		dev:     uint64(st.Dev),
		ino:     uint64(st.Ino),
		size:    fi.Size(),
		mtimeNS: mtimeNS,
		ctimeNS: ctimeNS,
	}, nil
}

// recheckTrust re-verifies the executable and manifest immediately before
// execution. Any difference from the state recorded at load — resolved path,
// ownership or modes, file identity, or manifest bytes — is reported as
// ErrAdapterChanged so the caller never execs a drifted adapter.
func (m *AdapterManifest) recheckTrust() error {
	if err := securePath(m.manifestPath); err != nil {
		return fmt.Errorf("%w: manifest: %v", ErrAdapterChanged, err)
	}
	data, err := os.ReadFile(m.manifestPath)
	if err != nil {
		return fmt.Errorf("%w: manifest: %v", ErrAdapterChanged, err)
	}
	if sha256.Sum256(data) != m.digest {
		return fmt.Errorf("%w: manifest %s changed since load", ErrAdapterChanged, m.manifestPath)
	}
	resolved, id, err := resolveSecureExecutable(m.Executable)
	if err != nil {
		return fmt.Errorf("%w: executable: %v", ErrAdapterChanged, err)
	}
	if resolved != m.ResolvedPath || id.dev != m.Dev || id.ino != m.Ino ||
		id.size != m.Size || id.mtimeNS != m.MtimeNS || id.ctimeNS != m.CtimeNS {
		return fmt.Errorf("%w: executable %s changed since load", ErrAdapterChanged, m.Executable)
	}
	return nil
}

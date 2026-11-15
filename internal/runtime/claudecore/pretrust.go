package claudecore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// trustedProjectsField is the per-project flag Claude Code reads to decide
// whether to show the "Do you trust the files in this folder?" dialog.
const trustedProjectsField = "hasTrustDialogAccepted"

// legacyConfigName is the older name of Claude Code's global config file. It
// takes precedence over the current one whenever it exists and is non-empty.
const legacyConfigName = ".config.json"

// trustConfigAttempts bounds how many times a concurrent write to Claude's
// config is retried before the edit gives up.
const trustConfigAttempts = 5

// Expected refusals: directories avenor does not itself persist trust for.
// They stay distinct so tests can tell a refusal from a successful edit, while
// PreAcceptTrust reports them as success — the dialog then appears and the
// session's dialog handler answers it, and Claude Code stores the trust
// itself.
var (
	errRefuseRoot                = errors.New("refusing to trust the filesystem root")
	errRefuseHome                = errors.New("refusing to trust a directory that is or contains the home directory")
	errRefuseWorldWritableSticky = errors.New("refusing to trust a world-writable sticky directory")
)

// The config lock wait: Claude Code retries its own lock with 200ms–4s
// backoff. Avenor retries short and gives up at about 1s so a held lock falls
// back to the dialog rather than delaying the launch.
const (
	trustLockMinBackoff = 10 * time.Millisecond
	trustLockMaxBackoff = 200 * time.Millisecond
	trustLockTotalWait  = time.Second
)

// trustLockRenameWindow is how long an edit may hold the config lock before it
// must give up. Claude Code takes over a lock older than 10s, so an edit that
// renamed the file past that point would write under a lock it no longer
// owns.
const trustLockRenameWindow = 5 * time.Second

// trustClock supplies the times the lock wait and the rename window compare. A
// test swaps it to stand in for a stalled edit without sleeping.
var trustClock = time.Now

// passwdHome resolves the home directory from the passwd entry, for the
// environments where $HOME is unset. A test swaps it so a refusal case never
// depends on this machine's real home directory.
var passwdHome = func() (string, error) {
	account, err := user.Current()
	if err != nil {
		return "", err
	}
	return account.HomeDir, nil
}

// trustFileOwner reports the uid that owns info, when the platform exposes one.
// A test swaps it to stand in for a config owned by another user.
var trustFileOwner = func(info fs.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

// PreAcceptTrust records that Claude Code may trust dir, so its startup trust
// dialog does not appear. It edits Claude Code's global config in place,
// leaving every other field as it was.
//
// The config is $CLAUDE_CONFIG_DIR/.claude.json when that variable is set, and
// ~/.claude.json otherwise. A missing config is left alone: it means Claude
// Code's onboarding has not happened yet, and avenor does not create it. So is
// a configuration the run's Claude Code would read elsewhere (see
// trustEditSkipped): marking trust in a file it never reads would only look
// like success.
//
// Failures are not fatal to the caller. Any error means the config was not
// changed, so Claude Code shows the dialog and the session's key-driven
// handler answers it. A cancelled ctx ends a lock wait early.
func PreAcceptTrust(ctx context.Context, dir string) error {
	if strings.TrimSpace(dir) == "" {
		return errors.New("empty run directory")
	}
	home, err := resolveHomeDir()
	if err != nil {
		return err
	}
	configHome, err := claudeConfigHome(home)
	if err != nil {
		return err
	}
	if trustEditSkipped(configHome) {
		return nil
	}
	err = preAcceptTrust(ctx, filepath.Join(configHome, ".claude.json"), home, dir)
	if isTrustRefusal(err) {
		return nil
	}
	return err
}

// isTrustRefusal reports whether err is one of the expected refusals.
func isTrustRefusal(err error) bool {
	return errors.Is(err, errRefuseRoot) ||
		errors.Is(err, errRefuseHome) ||
		errors.Is(err, errRefuseWorldWritableSticky)
}

// preAcceptTrust is PreAcceptTrust with the config path, home, and directory
// supplied by the caller, so tests never depend on the environment.
func preAcceptTrust(ctx context.Context, configPath, home, dir string) error {
	key, err := trustKey(dir)
	if err != nil {
		return err
	}
	if err := checkTrustTarget(key, home); err != nil {
		return err
	}
	return editTrustConfig(ctx, configPath, key, nil)
}

// resolveHomeDir is the home directory the refusals compare against.
// os.UserHomeDir reads $HOME, which a bare service environment may not set, so
// the passwd entry is the fallback. No home at all is an error rather than a
// skip: without it the home and home-parent refusals cannot be applied.
func resolveHomeDir() (string, error) {
	if home, err := os.UserHomeDir(); err == nil && strings.TrimSpace(home) != "" {
		return home, nil
	}
	home, err := passwdHome()
	if err != nil {
		return "", fmt.Errorf("home directory: %w", err)
	}
	if strings.TrimSpace(home) == "" {
		return "", errors.New("home directory: none found")
	}
	return home, nil
}

// trustKey is the project key Claude Code files trust under: the run
// directory's absolute path with symlinks resolved. Claude resolves its cwd
// through the kernel before keying, so a config entry for a symlink spelling
// of the directory would never match.
func trustKey(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("absolute path for %s: %w", dir, err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", dir, err)
	}
	return filepath.Clean(resolved), nil
}

// checkTrustTarget refuses the directories avenor itself never persists trust
// for: the filesystem root, the home directory or a directory that contains
// it, and a world-writable sticky directory such as /tmp, where any local user
// could plant a .claude/settings.json.
//
// The refusal does not by itself keep trust off these directories. It only
// says avenor writes no record for them: PreAcceptTrust reports it as success,
// the dialog appears, and the session's dialog handler answers it, so Claude
// Code persists the trust itself, exactly as it would without avenor. What
// that handler does in these directories is separate from this check.
func checkTrustTarget(key, home string) error {
	if key == string(filepath.Separator) || key == filepath.VolumeName(key)+string(filepath.Separator) {
		return fmt.Errorf("%w: %s", errRefuseRoot, key)
	}
	if info, err := os.Stat(key); err == nil && info.Mode()&os.ModeSticky != 0 && info.Mode().Perm()&0o002 != 0 {
		return fmt.Errorf("%w: %s", errRefuseWorldWritableSticky, key)
	}
	if home == "" {
		return nil
	}
	homeAbs, err := filepath.Abs(home)
	if err != nil {
		return nil
	}
	if resolved, err := filepath.EvalSymlinks(homeAbs); err == nil {
		homeAbs = resolved
	}
	homeAbs = filepath.Clean(homeAbs)
	if key == homeAbs || strings.HasPrefix(homeAbs, key+string(filepath.Separator)) {
		return fmt.Errorf("%w: %s", errRefuseHome, key)
	}
	return nil
}

// claudeConfigHome is the directory Claude Code keeps its global config in:
// CLAUDE_CONFIG_DIR when set, otherwise the home directory. Claude Code treats
// an empty or relative CLAUDE_CONFIG_DIR differently from an unset one, so a
// relative value is refused rather than resolved against avenor's cwd.
func claudeConfigHome(home string) (string, error) {
	if configDir := os.Getenv("CLAUDE_CONFIG_DIR"); configDir != "" {
		if !filepath.IsAbs(configDir) {
			return "", fmt.Errorf("CLAUDE_CONFIG_DIR %q is not an absolute path", configDir)
		}
		return configDir, nil
	}
	return home, nil
}

// trustEditSkipped reports whether the run's Claude Code would read a config
// other than <config home>/.claude.json, in which case avenor writes no trust
// record and leaves the dialog handler to answer the launch.
//
// Claude Code 2.1.325 builds the global config name as `.claude${suffix}.json`,
// where the suffix is "-custom-oauth" (or a deployment suffix) when
// CLAUDE_CODE_CUSTOM_OAUTH_URL is set. It also prefers a legacy
// <config home>/.config.json whenever that file exists and is non-empty, and
// an empty one is ignored.
func trustEditSkipped(configHome string) bool {
	if os.Getenv("CLAUDE_CODE_CUSTOM_OAUTH_URL") != "" {
		return true
	}
	info, err := os.Stat(filepath.Join(configHome, legacyConfigName))
	return err == nil && info.Mode().IsRegular() && info.Size() > 0
}

// editTrustConfig edits one project's trust flag in the config at configPath.
// afterRead, when non-nil, runs after each read and before the replacement, so
// a test can simulate Claude Code writing the file in between.
//
// The edit holds Claude Code's own config lock from the read until after the
// rename, so a Claude writer that honors the same lock cannot interleave. The
// lock is a directory at <config path>.lock, the proper-lockfile convention
// Claude Code uses: it builds the lock path from the unresolved config path,
// so this does too. It orders this edit only against writers that take the
// same lock; a writer that ignores it can still land between the final stat
// and the rename, and that write is replaced by this edit.
//
// The config is re-read and the edit rebuilt whenever the file changed between
// the read and the rename, up to trustConfigAttempts. Change detection compares
// size, mtime, and file identity: Claude replaces the file by temp-file and
// rename, so a same-size rewrite inside one mtime tick is visible only through
// the inode.
//
// A config owned by another user is not touched: the replacement file would
// belong to avenor, so a root or sudo run would take the user's config over.
// An edit that has held the lock past trustLockRenameWindow gives up as well,
// because Claude Code may have taken the lock over by then.
func editTrustConfig(ctx context.Context, configPath, projectKey string, afterRead func()) error {
	lock, err := acquireTrustLock(ctx, configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No config directory, so nothing to edit.
			return nil
		}
		return fmt.Errorf("lock %s: %w", configPath, err)
	}
	defer lock.release()

	path, err := filepath.EvalSymlinks(configPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("resolve %s: %w", configPath, err)
	}
	for attempt := 0; attempt < trustConfigAttempts; attempt++ {
		raw, info, err := readTrustConfig(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", configPath, err)
		}
		if owner, ok := trustFileOwner(info); ok && owner != uint32(os.Geteuid()) {
			return fmt.Errorf("%s is owned by uid %d, not uid %d; left it untouched", configPath, owner, os.Geteuid())
		}
		if afterRead != nil {
			afterRead()
		}
		updated, changed, err := markProjectTrusted(raw, projectKey)
		if err != nil {
			return fmt.Errorf("%s: %w", configPath, err)
		}
		if !changed {
			return nil
		}
		if held := trustClock().Sub(lock.acquiredAt); held > trustLockRenameWindow {
			return fmt.Errorf("held the lock on %s for %s; left it untouched", configPath, held.Round(time.Millisecond))
		}
		replaced, err := replaceTrustConfig(path, updated, info)
		if err != nil {
			return fmt.Errorf("write %s: %w", configPath, err)
		}
		if replaced {
			return nil
		}
	}
	return fmt.Errorf("%s changed while it was being updated; left it untouched", configPath)
}

// trustLock is a config lock avenor holds: the lock directory it created, with
// the identity and mtime recorded right after the mkdir.
type trustLock struct {
	path       string
	info       fs.FileInfo
	acquiredAt time.Time
	released   bool
}

// release removes the lock directory only while it is still the one avenor
// created. Claude Code's proper-lockfile takes over a lock older than 10s, so
// a stalled avenor that removed the directory afterwards would remove a lock
// Claude Code now owns; the lock is left in place instead.
func (l *trustLock) release() {
	if l.released {
		return
	}
	l.released = true
	current, err := os.Stat(l.path)
	if err != nil {
		return
	}
	if !os.SameFile(current, l.info) || !current.ModTime().Equal(l.info.ModTime()) {
		return
	}
	_ = os.Remove(l.path)
}

// acquireTrustLock creates Claude Code's lock directory for the config at
// configPath. It never removes or steals a lock it did not create: on EEXIST
// it retries with backoff for trustLockTotalWait and then reports the lock as
// held, which falls back to the dialog. A cancelled ctx ends the wait at once
// instead of holding the caller for the whole timeout.
func acquireTrustLock(ctx context.Context, configPath string) (*trustLock, error) {
	lockPath := configPath + ".lock"
	deadline := trustClock().Add(trustLockTotalWait)
	backoff := trustLockMinBackoff
	for {
		err := os.Mkdir(lockPath, 0o755)
		if err == nil {
			info, err := os.Stat(lockPath)
			if err != nil {
				_ = os.Remove(lockPath)
				return nil, err
			}
			return &trustLock{path: lockPath, info: info, acquiredAt: trustClock()}, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		if !trustClock().Before(deadline) {
			return nil, fmt.Errorf("config lock %s is held by another process", lockPath)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff *= 2; backoff > trustLockMaxBackoff {
			backoff = trustLockMaxBackoff
		}
	}
}

// readTrustConfig reads the config through one handle so the size, mtime, and
// identity used for change detection belong to the bytes that were read.
func readTrustConfig(path string) ([]byte, fs.FileInfo, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("not a regular file")
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}
	return raw, info, nil
}

// markProjectTrusted sets the trust flag on one project entry, creating the
// entry when it is absent. It edits map[string]json.RawMessage at each level,
// so unknown fields and the exact spelling of numbers survive.
//
// A project entry that already carries a true flag is left alone and reports
// changed=false, so an already trusted directory never rewrites the file. Key
// order in the output follows Go's map ordering and values are re-indented;
// their contents are otherwise preserved.
func markProjectTrusted(raw []byte, projectKey string) ([]byte, bool, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, false, fmt.Errorf("config is not valid JSON: %w", err)
	}
	if doc == nil {
		return nil, false, errors.New("config is not a JSON object")
	}

	projects := map[string]json.RawMessage{}
	if encoded, ok := doc["projects"]; ok {
		if err := json.Unmarshal(encoded, &projects); err != nil {
			return nil, false, fmt.Errorf("projects is not a JSON object: %w", err)
		}
		if projects == nil {
			return nil, false, errors.New("projects is not a JSON object")
		}
	}

	entry := map[string]json.RawMessage{}
	if encoded, ok := projects[projectKey]; ok {
		if err := json.Unmarshal(encoded, &entry); err != nil {
			return nil, false, fmt.Errorf("projects[%q] is not a JSON object: %w", projectKey, err)
		}
		if entry == nil {
			return nil, false, fmt.Errorf("projects[%q] is not a JSON object", projectKey)
		}
	}
	if accepted, ok := entry[trustedProjectsField]; ok {
		var trusted bool
		if json.Unmarshal(accepted, &trusted) == nil && trusted {
			return nil, false, nil
		}
	}

	entry[trustedProjectsField] = json.RawMessage("true")
	encodedEntry, err := encodeTrustJSON(entry, "")
	if err != nil {
		return nil, false, err
	}
	projects[projectKey] = encodedEntry
	encodedProjects, err := encodeTrustJSON(projects, "")
	if err != nil {
		return nil, false, err
	}
	doc["projects"] = encodedProjects

	// Two-space indentation matches Claude Code's own formatting, so the parts
	// of the file this edit does not touch keep their shape.
	updated, err := encodeTrustJSON(doc, "  ")
	if err != nil {
		return nil, false, err
	}
	return updated, true, nil
}

// encodeTrustJSON encodes v without HTML escaping. The default encoder would
// rewrite "&", "<" and ">" inside every untouched string value as \u0026,
// \u003c and \u003e, changing the spelling of fields this edit does not own.
// A non-empty indent selects indented output.
func encodeTrustJSON(v any, indent string) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if indent != "" {
		enc.SetIndent("", indent)
	}
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// replaceTrustConfig writes updated to a temp file beside path, fsyncs it, and
// renames it over path. It reports false, removing the temp file, when path
// changed since info was read.
func replaceTrustConfig(path string, updated []byte, info fs.FileInfo) (bool, error) {
	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return false, err
	}
	tempName := temp.Name()
	discard := func() {
		_ = temp.Close()
		_ = os.Remove(tempName)
	}
	if _, err := temp.Write(updated); err != nil {
		discard()
		return false, err
	}
	if err := temp.Chmod(info.Mode().Perm()); err != nil {
		discard()
		return false, err
	}
	if err := temp.Sync(); err != nil {
		discard()
		return false, err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempName)
		return false, err
	}

	current, err := os.Stat(path)
	if err != nil {
		_ = os.Remove(tempName)
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if current.Size() != info.Size() || !current.ModTime().Equal(info.ModTime()) || !os.SameFile(current, info) {
		_ = os.Remove(tempName)
		return false, nil
	}
	if err := os.Rename(tempName, path); err != nil {
		_ = os.Remove(tempName)
		return false, err
	}
	syncTrustDir(filepath.Dir(path))
	return true, nil
}

// syncTrustDir flushes the directory entry after the rename. A failure only
// weakens durability across a crash, so it is not reported.
func syncTrustDir(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = handle.Sync()
	_ = handle.Close()
}

package control

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// OwnerCapability is the bearer credential of the process that owns a control
// server. The ID names the capability; the secret authenticates it.
type OwnerCapability struct {
	ID     string
	Secret string
}

// GenerateOwnerCapability mints a fresh 128-bit capability ID and 256-bit
// secret. Both are lowercase hex.
func GenerateOwnerCapability() (OwnerCapability, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return OwnerCapability{}, fmt.Errorf("generate owner capability id: %w", err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return OwnerCapability{}, fmt.Errorf("generate owner capability secret: %w", err)
	}
	return OwnerCapability{ID: hex.EncodeToString(id), Secret: hex.EncodeToString(secret)}, nil
}

const (
	ownerRecordVersion  = 1
	ownerRecordMaxBytes = 64 << 10
)

// OwnerRecord is the published record beside a control socket.
type OwnerRecord struct {
	Version        int            `json:"version"`
	Socket         string         `json:"socket"`
	SocketIdentity SocketIdentity `json:"socket_identity"`
	CapabilityID   string         `json:"capability_id"`
	Secret         string         `json:"secret"`
	OwnerPID       int            `json:"owner_pid"`
}

// SocketIdentity is a device/inode pair encoded as decimal strings so it
// survives JSON readers that truncate integers.
type SocketIdentity struct {
	Dev string `json:"dev"`
	Ino string `json:"ino"`
}

// Errors a record reader classifies instead of guessing. stale-owner-record is
// never a wire code: it is raised before anything is sent.
var (
	ErrSocketInUse        = errors.New("control socket already in use")
	ErrOwnerRecordMissing = errors.New("owner record missing")
	ErrStaleOwnerRecord   = errors.New("stale owner record")
)

// Seams the publication contract's tests replace. Production leaves them at
// their defaults.
var (
	beforeOwnerRecordOpen    = func() {}
	beforeOwnerRecordPublish = func(string) error { return nil }
	testOwnerRecordRemove    = os.Remove
	testOwnerSocketRemove    = os.Remove
)

// OwnerRecordPath is the record published beside socketPath.
func OwnerRecordPath(socketPath string) string { return socketPath + ".owner.json" }

func socketIdentity(info os.FileInfo) (SocketIdentity, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return SocketIdentity{}, fmt.Errorf("stat socket: unsupported stat type %T", info.Sys())
	}
	return SocketIdentity{
		Dev: strconv.FormatUint(uint64(st.Dev), 10),
		Ino: strconv.FormatUint(st.Ino, 10),
	}, nil
}

// publishOwnerRecord writes this server's credential beside socketPath. The
// caller holds the control lock and the listener is live.
func (s *ControlServer) publishOwnerRecord(socketPath string, identity SocketIdentity) error {
	if err := beforeOwnerRecordPublish(socketPath); err != nil {
		return err
	}
	recordPath := OwnerRecordPath(socketPath)
	if err := rejectSymlinkedParent(recordPath); err != nil {
		return err
	}
	body, err := json.Marshal(OwnerRecord{
		Version:        ownerRecordVersion,
		Socket:         socketPath,
		SocketIdentity: identity,
		CapabilityID:   s.owner.ID,
		Secret:         s.owner.Secret,
		OwnerPID:       os.Getpid(),
	})
	if err != nil {
		return fmt.Errorf("encode owner record: %w", err)
	}

	// The exclusive temp file cannot land on a symlink or a pre-existing entry;
	// the rename that follows still replaces recordPath, and the record only
	// becomes visible once its contents are synced.
	if err := durablefile.WriteFileAtomic(recordPath, durablefile.TempPattern(recordPath), body, 0o600); err != nil {
		return fmt.Errorf("publish owner record: %w", err)
	}
	if _, err := inspectRecord(recordPath); err != nil {
		return err
	}
	return nil
}

// inspectRecord applies the file-level publication contract to path.
func inspectRecord(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrOwnerRecordMissing, path)
		}
		return nil, fmt.Errorf("%w: stat %s: %v", ErrStaleOwnerRecord, path, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrStaleOwnerRecord, path)
	}
	if info.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("%w: %s has mode %04o, want 0600", ErrStaleOwnerRecord, path, info.Mode().Perm())
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%w: %s is not owned by uid %d", ErrStaleOwnerRecord, path, os.Geteuid())
	}
	if info.Size() > ownerRecordMaxBytes {
		return nil, fmt.Errorf("%w: %s is too large", ErrStaleOwnerRecord, path)
	}
	return info, nil
}

// rejectSymlinkedParent refuses a record whose immediate parent directory is a
// symlink. The record file itself is checked by inspectRecord; ancestors above
// the parent are resolved, not validated, because a temp directory can reach
// its real location through one (macOS /tmp and $TMPDIR both do).
func rejectSymlinkedParent(path string) error {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("stat %s: %w", parent, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("owner record path %s has a symlinked parent %s", path, parent)
	}
	return nil
}

// LoadOwnerRecord validates and decodes the record published beside socketPath.
func LoadOwnerRecord(socketPath string) (OwnerRecord, error) {
	record, _, err := loadOwnerRecord(socketPath)
	return record, err
}

// loadOwnerRecord also returns the file the publication contract inspected, so
// a caller that acts on the record afterwards can confirm the path still names
// that same file.
func loadOwnerRecord(socketPath string) (OwnerRecord, os.FileInfo, error) {
	path := OwnerRecordPath(socketPath)
	if err := rejectSymlinkedParent(path); err != nil {
		return OwnerRecord{}, nil, fmt.Errorf("%w: %v", ErrStaleOwnerRecord, err)
	}
	inspected, err := inspectRecord(path)
	if err != nil {
		return OwnerRecord{}, nil, err
	}
	beforeOwnerRecordOpen()
	file, err := os.Open(path)
	if err != nil {
		return OwnerRecord{}, nil, fmt.Errorf("%w: open %s: %v", ErrStaleOwnerRecord, path, err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return OwnerRecord{}, nil, fmt.Errorf("%w: stat %s: %v", ErrStaleOwnerRecord, path, err)
	}
	if !os.SameFile(inspected, opened) {
		return OwnerRecord{}, nil, fmt.Errorf("%w: %s changed while reading", ErrStaleOwnerRecord, path)
	}

	decoder := json.NewDecoder(io.LimitReader(file, ownerRecordMaxBytes))
	decoder.DisallowUnknownFields()
	var record OwnerRecord
	if err := decoder.Decode(&record); err != nil {
		return OwnerRecord{}, nil, fmt.Errorf("%w: decode %s: %v", ErrStaleOwnerRecord, path, err)
	}
	if record.Version != ownerRecordVersion {
		return OwnerRecord{}, nil, fmt.Errorf("%w: %s has version %d", ErrStaleOwnerRecord, path, record.Version)
	}
	if record.Socket != socketPath {
		return OwnerRecord{}, nil, fmt.Errorf("%w: %s names socket %s", ErrStaleOwnerRecord, path, record.Socket)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return OwnerRecord{}, nil, fmt.Errorf("%w: %s has trailing data", ErrStaleOwnerRecord, path)
	}
	return record, inspected, nil
}

// removeOwnerRecord removes the record only when it still names this server's
// socket identity and capability. The guard reads one specific file, and the
// path is re-inspected immediately before the unlink: removal happens only
// when it still names the file the guard read, so a replacement that renamed a
// fresh record into place keeps it. A missing record is idempotent success.
func (s *ControlServer) removeOwnerRecord(socketPath string, identity SocketIdentity, capabilityID string) error {
	record, inspected, err := loadOwnerRecord(socketPath)
	if err != nil {
		return nil
	}
	if record.CapabilityID != capabilityID || record.SocketIdentity != identity {
		return nil
	}
	recordPath := OwnerRecordPath(socketPath)
	current, err := os.Lstat(recordPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat owner record: %w", err)
	}
	if !os.SameFile(inspected, current) {
		return nil
	}
	if err := testOwnerRecordRemove(recordPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove owner record: %w", err)
	}
	return nil
}

// OwnerFacade is the narrow owner-authorized surface the HTTP debug adapter
// needs.
type OwnerFacade interface {
	Cancel()
	// AnswerPermission returns the server's PermissionAnswerDelivery unchanged.
	AnswerPermission(scope, requestID, optionID, message string) PermissionAnswerDelivery
}

// OwnerFacade returns nil only for a zero-value ControlServer.
func (s *ControlServer) OwnerFacade() OwnerFacade {
	if s == nil || s.owner.ID == "" {
		return nil
	}
	return ownerFacade{server: s}
}

type ownerFacade struct{ server *ControlServer }

func (f ownerFacade) Cancel() {
	if fn := f.server.cancelFn; fn != nil {
		fn()
	}
}

func (f ownerFacade) AnswerPermission(scope, requestID, optionID, message string) PermissionAnswerDelivery {
	return f.server.DeliverPendingPermission(scope, requestID, optionID, message)
}

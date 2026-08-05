// Package admission implements a tree-scoped execution-admission controller for
// stable supervisor trees.
//
// The admission controller answers a single question: "may this runtime consume
// execution capacity now?" It bounds the total number of concurrent runtimes
// across an entire supervisor tree, including runtimes started by nested
// supervisors. It is deliberately not a workflow scheduler, priority policy, or
// durable work queue.
//
// The budget is backed by a single JSON file protected by an exclusive flock so
// that sibling and nested supervisors in separate processes can atomically
// reserve and release capacity. Each reservation records the PID of the
// process that holds it so a root's reaper can reclaim capacity from
// orphaned or crashed descendants.
package admission

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// EnvTreeBudget is the environment variable that carries the root budget file
// path to nested supervisors. A root supervisor's command entry point sets it
// so descendant processes inherit the tree budget identity.
const EnvTreeBudget = "AVENOR_TREE_BUDGET"

// DefaultTreeBudget is the default descendant budget capacity for a root
// supervisor tree. It is intentionally larger than the local fan-out default
// (16) so that ordinary nesting has headroom while recursive multiplication is
// still bounded.
const DefaultTreeBudget = 64

// CapacityError is a typed, retryable admission failure. Callers that can wait
// may retry after a capacity change; callers that cannot wait surface this to
// their user as a retryable condition rather than a runtime failure.
type CapacityError struct {
	// Source is "local" for the per-supervisor fan-out limit or "tree" for the
	// inherited descendant budget.
	Source string `json:"source"`
	// Limit is the capacity that was exhausted.
	Limit int `json:"limit"`
	// Active is the number of runtimes currently consuming capacity.
	Active int `json:"active"`
	// RootID is the tree budget root identity (only set for tree exhaustion).
	RootID string `json:"root_id,omitempty"`
}

func (e *CapacityError) Error() string {
	if e.Source == "local" {
		return fmt.Sprintf("max runtimes (%d) reached", e.Limit)
	}
	return fmt.Sprintf("tree budget exhausted (%d/%d active)", e.Active, e.Limit)
}

// Retryable reports that capacity may become available when a runtime
// completes or a stale descendant is reaped.
func (e *CapacityError) Retryable() bool { return true }

// reservation records a single held capacity slot.
type reservation struct {
	PID    int    `json:"pid"`
	Holder string `json:"holder"`
	At     int64  `json:"at"` // unix nanoseconds
	Token  string `json:"token"`
}

type budgetFile struct {
	RootID   string                  `json:"root_id"`
	Capacity int                     `json:"capacity"`
	Active   map[string]*reservation `json:"active"`
}

// Budget is a tree-scoped admission authority backed by a flock-protected
// file. A root Budget owns the file; a nested Budget opens the same file to
// share the tree's capacity.
type Budget struct {
	path   string
	rootID string
	// capacity is only meaningful for the root that created the file. Nested
	// participants read it from the file on every operation.
	capacity int
	isRoot   bool

	mu sync.Mutex // guards the file handle for concurrent in-process callers
	f  *os.File

	// notifyMu guards the list of in-process capacity-change callbacks. When a
	// token is released or reaped, all registered notifiers are called so that
	// supervisors sharing the same budget (in-process) can wake their waiters.
	// Cross-process notification is best-effort via WaitForCapacity polling.
	notifyMu  sync.Mutex
	notifiers []func()
}

// CreateRootInRuntimeState creates a root tree budget in Avenor-owned runtime
// state. It deliberately does not derive the path from a caller-provided
// control socket, which may be in a project or worktree. The returned root
// owns the file and removes it when closed.
func CreateRootInRuntimeState(capacity int) (*Budget, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("admission: get home directory: %w", err)
	}
	runtimeStateDir := filepath.Join(home, ".avenor", "sockets")
	if err := os.MkdirAll(runtimeStateDir, 0o700); err != nil {
		return nil, fmt.Errorf("admission: create runtime state directory: %w", err)
	}

	// CreateRoot uses O_EXCL, and the cryptographically random suffix keeps
	// independent root supervisors from contending for a predictable name.
	suffix, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("admission: generate runtime state name: %w", err)
	}
	path := filepath.Join(runtimeStateDir, fmt.Sprintf("tree-budget-%d-%s.tree-budget", os.Getpid(), suffix))
	return CreateRoot(path, capacity)
}

// CreateRoot creates a new tree budget file at path and returns a root Budget
// that owns it. The capacity is the maximum number of concurrent runtimes
// across the whole supervisor tree.
func CreateRoot(path string, capacity int) (*Budget, error) {
	if capacity <= 0 {
		capacity = DefaultTreeBudget
	}
	rootID, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("admission: generate root id: %w", err)
	}
	bf := budgetFile{
		RootID:   rootID,
		Capacity: capacity,
		Active:   map[string]*reservation{},
	}
	data, err := json.MarshalIndent(&bf, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("admission: marshal budget: %w", err)
	}
	// Create with O_EXCL|O_NOFOLLOW to refuse a pre-placed symlink or existing
	// file, preventing a symlink-following overwrite attack on the selected
	// budget path.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("admission: create budget file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("admission: write budget file: %w", err)
	}
	return &Budget{
		path:     path,
		rootID:   rootID,
		capacity: capacity,
		isRoot:   true,
		f:        f,
	}, nil
}

// Open joins an existing tree budget file as a nested participant. The
// returned Budget shares the root's capacity across processes.
func Open(path string) (*Budget, error) {
	// O_NOFOLLOW refuses a symlink so a nested supervisor cannot be tricked into
	// writing through a symlink planted at the inherited budget path.
	f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("admission: open budget file: %w", err)
	}
	b := &Budget{path: path, isRoot: false, f: f}
	// Read the root identity once for diagnostics; capacity is read fresh on
	// every Acquire so a root resize (future) is observed.
	bf, err := b.readWithFlock()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	b.rootID = bf.RootID
	b.capacity = bf.Capacity
	return b, nil
}

// Path returns the budget file path.
func (b *Budget) Path() string { return b.path }

// RawFile returns the underlying file handle for test-only manipulation.
// Production code must use Acquire/Release/Reap/Status.
func (b *Budget) RawFile() *os.File {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f
}

// RootID returns the tree budget root identity.
func (b *Budget) RootID() string { return b.rootID }

// Close releases the underlying file handle. A root budget also removes its
// file so a shut-down supervisor tree does not leave a stale budget behind.
func (b *Budget) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil {
		return nil
	}
	path := b.path
	isRoot := b.isRoot
	err := b.f.Close()
	b.f = nil
	if err != nil {
		return err
	}
	if isRoot {
		_ = os.Remove(path)
	}
	return nil
}

// AddNotifier registers a callback invoked when a token is released or reaped.
// Multiple supervisors sharing the same budget (in-process) register here so a
// release in one wakes capacity waiters in the others.
func (b *Budget) AddNotifier(f func()) {
	b.notifyMu.Lock()
	b.notifiers = append(b.notifiers, f)
	b.notifyMu.Unlock()
}

// notifyAll calls every registered notifier. It is invoked after a successful
// Release or Reap so capacity waiters can retry.
func (b *Budget) notifyAll() {
	b.notifyMu.Lock()
	notifiers := append([]func(){}, b.notifiers...)
	b.notifyMu.Unlock()
	for _, f := range notifiers {
		f()
	}
}

// Acquire atomically reserves one capacity slot. It returns a token that must
// be passed to Release. If the tree budget is exhausted it returns a
// *CapacityError with Source "tree".
//
// holder is a diagnostic label (e.g. the runtime ID) recorded with the
// reservation; it is not used for release matching.
func (b *Budget) Acquire(holder string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil {
		return "", errors.New("admission: budget is closed")
	}
	if err := lock(b.f); err != nil {
		return "", err
	}
	defer unlock(b.f)

	bf, err := readBudget(b.f)
	if err != nil {
		return "", err
	}
	if bf.Active == nil {
		bf.Active = map[string]*reservation{}
	}
	if len(bf.Active) >= bf.Capacity {
		return "", &CapacityError{
			Source: "tree",
			Limit:  bf.Capacity,
			Active: len(bf.Active),
			RootID: bf.RootID,
		}
	}
	token, err := newToken()
	if err != nil {
		return "", fmt.Errorf("admission: generate token: %w", err)
	}
	bf.Active[token] = &reservation{
		PID:    os.Getpid(),
		Holder: holder,
		At:     time.Now().UnixNano(),
		Token:  token,
	}
	if err := writeBudget(b.f, bf); err != nil {
		return "", err
	}
	return token, nil
}

// Release returns a held capacity slot to the tree budget. It is safe to call
// with an empty token (no-op) and to call more than once (idempotent).
func (b *Budget) Release(token string) {
	if token == "" {
		return
	}
	if b.releaseLocked(token) {
		b.notifyAll()
	}
}

func (b *Budget) releaseLocked(token string) bool {
	b.mu.Lock()
	if b.f == nil {
		b.mu.Unlock()
		return false
	}
	if err := lock(b.f); err != nil {
		b.mu.Unlock()
		return false
	}
	bf, err := readBudget(b.f)
	if err != nil {
		unlock(b.f)
		b.mu.Unlock()
		return false
	}
	if bf.Active == nil {
		unlock(b.f)
		b.mu.Unlock()
		return false
	}
	if _, ok := bf.Active[token]; !ok {
		unlock(b.f)
		b.mu.Unlock()
		return false
	}
	delete(bf.Active, token)
	if err := writeBudget(b.f, bf); err != nil {
		unlock(b.f)
		b.mu.Unlock()
		return false
	}
	unlock(b.f)
	b.mu.Unlock()
	return true
}

// Reap scans for reservations held by dead processes and reclaims their
// capacity. It returns the number of stale reservations removed. Only a root
// authority should need to reap, but any participant may call it safely.
func (b *Budget) Reap() int {
	b.mu.Lock()
	if b.f == nil {
		b.mu.Unlock()
		return 0
	}
	if err := lock(b.f); err != nil {
		b.mu.Unlock()
		return 0
	}
	bf, err := readBudget(b.f)
	if err != nil {
		unlock(b.f)
		b.mu.Unlock()
		return 0
	}
	if bf.Active == nil {
		unlock(b.f)
		b.mu.Unlock()
		return 0
	}
	reclaimed := 0
	for token, r := range bf.Active {
		if r.PID <= 0 {
			continue
		}
		if !pidAlive(r.PID) {
			delete(bf.Active, token)
			reclaimed++
		}
	}
	if reclaimed > 0 {
		if err := writeBudget(b.f, bf); err != nil {
			unlock(b.f)
			b.mu.Unlock()
			return 0
		}
	}
	unlock(b.f)
	b.mu.Unlock()
	if reclaimed > 0 {
		b.notifyAll()
	}
	return reclaimed
}

// Status returns the current active count, capacity, and root identity without
// modifying the budget.
func (b *Budget) Status() (active, capacity int, rootID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f == nil {
		return 0, b.capacity, b.rootID
	}
	if err := lock(b.f); err != nil {
		return 0, b.capacity, b.rootID
	}
	defer unlock(b.f)
	bf, err := readBudget(b.f)
	if err != nil {
		return 0, b.capacity, b.rootID
	}
	return len(bf.Active), bf.Capacity, bf.RootID
}

// readWithFlock reads the budget file after acquiring the flock itself.
func (b *Budget) readWithFlock() (*budgetFile, error) {
	if err := lock(b.f); err != nil {
		return nil, err
	}
	defer unlock(b.f)
	return readBudget(b.f)
}

func lock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
}

func unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

func readBudget(f *os.File) (*budgetFile, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("admission: seek: %w", err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("admission: read: %w", err)
	}
	if len(data) == 0 {
		return nil, errors.New("admission: budget file is empty")
	}
	var bf budgetFile
	if err := json.Unmarshal(data, &bf); err != nil {
		return nil, fmt.Errorf("admission: parse: %w", err)
	}
	return &bf, nil
}

func writeBudget(f *os.File, bf *budgetFile) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("admission: seek: %w", err)
	}
	data, err := json.MarshalIndent(bf, "", "  ")
	if err != nil {
		return fmt.Errorf("admission: marshal: %w", err)
	}
	n, err := f.Write(data)
	if err != nil {
		return fmt.Errorf("admission: write: %w", err)
	}
	// Truncate to the new length in case the file shrank.
	if err := f.Truncate(int64(n)); err != nil {
		return fmt.Errorf("admission: truncate: %w", err)
	}
	// Flush to disk so a crash leaves the budget file consistent rather
	// than partially written. The file is small so the fsync cost is low.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("admission: sync: %w", err)
	}
	return nil
}

func newToken() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}

// pidAlive reports whether a process with the given PID exists. A PID whose
// process has exited (ESRCH) is considered dead. EPERM (the process exists
// but is owned by another user) is treated as alive so the reaper never
// reclaims a live reservation and oversubscribes the budget. PID reuse is
// not a correctness risk to admission safety: a reused PID is alive, so a
// stale reservation is retained rather than reclaimed, which can only leak
// capacity (bounded by the budget) — never oversubscribe.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

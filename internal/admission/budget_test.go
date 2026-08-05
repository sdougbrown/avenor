package admission

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newBudgetFile(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "tree-budget.json")
}

func TestCreateRootInRuntimeState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	b, err := CreateRootInRuntimeState(4)
	if err != nil {
		t.Fatalf("CreateRootInRuntimeState: %v", err)
	}
	path := b.Path()
	defer b.Close()

	wantDir := filepath.Join(home, ".avenor", "sockets")
	if got := filepath.Dir(path); got != wantDir {
		t.Fatalf("budget directory = %q, want %q", got, wantDir)
	}
	if filepath.Ext(path) != ".tree-budget" {
		t.Fatalf("budget path = %q, want .tree-budget suffix", path)
	}
	if info, err := os.Stat(path); err != nil {
		t.Fatalf("stat budget file: %v", err)
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("budget mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestCreateRootInitialState(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 4)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	if !b.isRoot {
		t.Fatal("expected root budget")
	}
	if b.RootID() == "" {
		t.Fatal("root id is empty")
	}
	active, cap, rootID := b.Status()
	if active != 0 || cap != 4 || rootID == "" {
		t.Fatalf("Status = (%d, %d, %q), want (0, 4, non-empty)", active, cap, rootID)
	}
}

func TestCreateRootDefaultCapacity(t *testing.T) {
	path := newBudgetFile(t)
	for _, input := range []int{0, -1} {
		b, err := CreateRoot(path, input)
		if err != nil {
			t.Fatalf("CreateRoot(%d): %v", input, err)
		}
		_, cap, _ := b.Status()
		if cap != DefaultTreeBudget {
			b.Close()
			t.Fatalf("capacity(%d) = %d, want default %d", input, cap, DefaultTreeBudget)
		}
		if err := b.Close(); err != nil {
			t.Fatalf("Close(%d): %v", input, err)
		}
		_ = os.Remove(path)
	}
}

func TestAcquireUntilExhausted(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 3)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	tokens := make([]string, 3)
	for i := range tokens {
		tok, err := b.Acquire("rt")
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		tokens[i] = tok
	}
	active, _, _ := b.Status()
	if active != 3 {
		t.Fatalf("active = %d, want 3", active)
	}

	_, err = b.Acquire("rt")
	if err == nil {
		t.Fatal("expected capacity exhaustion error")
	}
	var ce *CapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("error type = %T, want *CapacityError", err)
	}
	if ce.Source != "tree" {
		t.Fatalf("source = %q, want %q", ce.Source, "tree")
	}
	if ce.Limit != 3 || ce.Active != 3 {
		t.Fatalf("CapacityError = %+v, want limit=3 active=3", ce)
	}
	if !ce.Retryable() {
		t.Fatal("CapacityError should be retryable")
	}
}

func TestReleaseFreesSlot(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	tok, err := b.Acquire("rt_1")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if _, err := b.Acquire("rt_2"); err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	b.Release(tok)
	active, _, _ := b.Status()
	if active != 1 {
		t.Fatalf("after release active = %d, want 1", active)
	}
	// Slot is available again.
	if _, err := b.Acquire("rt_3"); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
}

func TestReleaseIdempotent(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	tok, _ := b.Acquire("rt")
	b.Release(tok)
	b.Release(tok) // must not panic or corrupt
	b.Release("")  // no-op
	active, _, _ := b.Status()
	if active != 0 {
		t.Fatalf("active = %d, want 0", active)
	}
}

func TestOpenJoinsSharedBudget(t *testing.T) {
	path := newBudgetFile(t)
	root, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer root.Close()

	nested, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer nested.Close()

	if nested.isRoot {
		t.Fatal("nested should not be root")
	}
	if nested.RootID() != root.RootID() {
		t.Fatalf("root id mismatch: %q vs %q", nested.RootID(), root.RootID())
	}

	// Root holds one slot; nested should see only one remaining.
	if _, err := root.Acquire("root_rt"); err != nil {
		t.Fatalf("root Acquire: %v", err)
	}
	_, err = nested.Acquire("nested_rt")
	if err != nil {
		t.Fatalf("nested Acquire: %v", err)
	}
	_, err = nested.Acquire("nested_rt2")
	if err == nil {
		t.Fatal("nested should see budget exhausted across processes")
	}
}

func TestConcurrentAcquireNoOversubscribe(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 50)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	var wg sync.WaitGroup
	tokens := make(chan string, 200)
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tok, err := b.Acquire("rt")
			if err != nil {
				errs <- err
				return
			}
			tokens <- tok
		}()
	}
	wg.Wait()
	close(tokens)
	close(errs)

	acquired := 0
	for range tokens {
		acquired++
	}
	exhausted := 0
	for err := range errs {
		var ce *CapacityError
		if !errors.As(err, &ce) {
			t.Fatalf("unexpected error: %v", err)
		}
		exhausted++
	}
	if acquired != 50 {
		t.Fatalf("acquired = %d, want 50 (capacity)", acquired)
	}
	if exhausted != 150 {
		t.Fatalf("exhausted = %d, want 150", exhausted)
	}
	active, _, _ := b.Status()
	if active != 50 {
		t.Fatalf("active = %d, want 50", active)
	}
}

func TestReapRemovesDeadProcess(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 4)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	// Manually inject a reservation for a dead PID by writing the file.
	if err := lock(b.f); err != nil {
		t.Fatal(err)
	}
	bf, _ := readBudget(b.f)
	bf.Active["dead-tok"] = &reservation{PID: 999999, Holder: "dead", Token: "dead-tok"}
	_ = writeBudget(b.f, bf)
	unlock(b.f)

	active, _, _ := b.Status()
	if active != 1 {
		t.Fatalf("pre-reap active = %d, want 1", active)
	}

	reclaimed := b.Reap()
	if reclaimed != 1 {
		t.Fatalf("reclaimed = %d, want 1", reclaimed)
	}
	active, _, _ = b.Status()
	if active != 0 {
		t.Fatalf("post-reap active = %d, want 0", active)
	}
}

func TestReleaseDoesNotNotifyWhenPersistFails(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 1)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	tok, err := b.Acquire("rt")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	ro, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	b.mu.Lock()
	original := b.f
	b.f = ro
	b.mu.Unlock()
	defer original.Close()

	notified := false
	b.AddNotifier(func() { notified = true })
	b.Release(tok)
	if notified {
		t.Fatal("Release notified waiters after write failure")
	}
	active, _, _ := b.Status()
	if active != 1 {
		t.Fatalf("active = %d, want 1 after failed release", active)
	}
}

func TestReapDoesNotNotifyWhenPersistFails(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 4)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	if err := lock(b.f); err != nil {
		t.Fatal(err)
	}
	bf, _ := readBudget(b.f)
	bf.Active["dead-tok"] = &reservation{PID: 999999, Holder: "dead", Token: "dead-tok"}
	_ = writeBudget(b.f, bf)
	unlock(b.f)

	ro, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	b.mu.Lock()
	original := b.f
	b.f = ro
	b.mu.Unlock()
	defer original.Close()

	notified := false
	b.AddNotifier(func() { notified = true })
	reclaimed := b.Reap()
	if reclaimed != 0 {
		t.Fatalf("reclaimed = %d, want 0 after failed persist", reclaimed)
	}
	if notified {
		t.Fatal("Reap notified waiters after write failure")
	}
	active, _, _ := b.Status()
	if active != 1 {
		t.Fatalf("active = %d, want 1 after failed reap", active)
	}
}

func TestReapKeepsLiveProcess(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 4)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()

	if _, err := b.Acquire("live"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	reclaimed := b.Reap()
	if reclaimed != 0 {
		t.Fatalf("reclaimed live self = %d, want 0", reclaimed)
	}
	active, _, _ := b.Status()
	if active != 1 {
		t.Fatalf("active = %d, want 1", active)
	}
}

func TestIndependentRootsDoNotShare(t *testing.T) {
	dir := t.TempDir()
	a, err := CreateRoot(filepath.Join(dir, "a.json"), 2)
	if err != nil {
		t.Fatalf("CreateRoot a: %v", err)
	}
	defer a.Close()
	b, err := CreateRoot(filepath.Join(dir, "b.json"), 2)
	if err != nil {
		t.Fatalf("CreateRoot b: %v", err)
	}
	defer b.Close()

	if a.RootID() == b.RootID() {
		t.Fatal("independent roots share a root id")
	}
	if _, err := a.Acquire("rt"); err != nil {
		t.Fatalf("a Acquire: %v", err)
	}
	if _, err := a.Acquire("rt"); err != nil {
		t.Fatalf("a Acquire 2: %v", err)
	}
	// b is independent and unaffected by a's exhaustion.
	if _, err := b.Acquire("rt"); err != nil {
		t.Fatalf("b Acquire should succeed independently: %v", err)
	}
}

func TestOpenMissingFileFails(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "nope.json"))
	if err == nil {
		t.Fatal("expected error opening missing budget file")
	}
}

func TestCloseReleasesHandle(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	rootID := b.RootID()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Operations after close are safe no-ops / errors.
	b.Release("anything")
	if active, cap, gotRootID := b.Status(); active != 0 || cap != 2 || gotRootID != rootID {
		t.Fatalf("Status after Close = (%d, %d, %q), want (0, 2, %q)", active, cap, gotRootID, rootID)
	}
	if _, err := b.Acquire("rt"); err == nil {
		t.Fatal("Acquire after Close should error")
	} else if !strings.Contains(err.Error(), "budget is closed") {
		t.Fatalf("Acquire error = %q, want to contain %q", err.Error(), "budget is closed")
	}
}

func TestAcquireReleaseAcrossOpenHandles(t *testing.T) {
	path := newBudgetFile(t)
	root, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer root.Close()

	nested, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer nested.Close()

	tok, err := root.Acquire("root_rt")
	if err != nil {
		t.Fatalf("root Acquire: %v", err)
	}
	// Releasing from a different handle that shares the file works.
	nested.Release(tok)
	active, _, _ := root.Status()
	if active != 0 {
		t.Fatalf("active = %d, want 0 after cross-handle release", active)
	}
}

func TestBudgetFilePermissions(t *testing.T) {
	path := newBudgetFile(t)
	b, err := CreateRoot(path, 2)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer b.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("perms = %o, want 0600", info.Mode().Perm())
	}
}

// TestReapReclaimsDeadPIDAcrossHandles simulates a nested supervisor that
// acquires a token and crashes. A second handle reaps the stale reservation.
func TestReapReclaimsDeadPIDAcrossHandles(t *testing.T) {
	path := newBudgetFile(t)
	root, err := CreateRoot(path, 4)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	defer root.Close()

	nested, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer nested.Close()

	// nested acquires a token then "crashes" — we simulate the crash by
	// corrupting its reservation's PID to a dead PID.
	tok, err := nested.Acquire("crashed-child")
	if err != nil {
		t.Fatalf("nested Acquire: %v", err)
	}
	_ = tok

	// Overwrite the reservation PID to a dead PID directly in the file.
	if err := lock(root.f); err != nil {
		t.Fatal(err)
	}
	bf, _ := readBudget(root.f)
	for _, r := range bf.Active {
		r.PID = 999999 // a PID that does not exist
	}
	_ = writeBudget(root.f, bf)
	unlock(root.f)

	active, _, _ := root.Status()
	if active != 1 {
		t.Fatalf("active = %d, want 1 before reap", active)
	}
	reclaimed := root.Reap()
	if reclaimed != 1 {
		t.Fatalf("reclaimed = %d, want 1", reclaimed)
	}
	active, _, _ = root.Status()
	if active != 0 {
		t.Fatalf("active = %d, want 0 after reap", active)
	}
}

func TestCapacityErrorFormat(t *testing.T) {
	local := &CapacityError{Source: "local", Limit: 16, Active: 16}
	if msg := local.Error(); msg != "max runtimes (16) reached" {
		t.Fatalf("local Error() = %q, want %q", msg, "max runtimes (16) reached")
	}
	if !local.Retryable() {
		t.Fatal("local CapacityError should be retryable")
	}

	tree := &CapacityError{Source: "tree", Limit: 64, Active: 64, RootID: "abc123"}
	if msg := tree.Error(); msg != "tree budget exhausted (64/64 active)" {
		t.Fatalf("tree Error() = %q, want %q", msg, "tree budget exhausted (64/64 active)")
	}
	if !tree.Retryable() {
		t.Fatal("tree CapacityError should be retryable")
	}
}

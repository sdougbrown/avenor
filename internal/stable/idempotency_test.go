package stable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/admission"
	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/events"
	"github.com/sdougbrown/avenor/internal/runtime"
)

// idempotencyTestProvider is a provider whose sessions never end: runtimes
// started through it stay active (counting against MaxRuntimes) for the
// duration of the test.
type idempotencyTestProvider struct {
	mu          sync.Mutex
	nextSession int
	startCalls  int
}

func (p *idempotencyTestProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	p.mu.Lock()
	p.startCalls++
	p.nextSession++
	id := p.nextSession
	p.mu.Unlock()
	return runtime.Session{SessionID: fmt.Sprintf("ses_idem_%d", id)}, nil
}

func (p *idempotencyTestProvider) Resume(context.Context, string) (runtime.Session, error) {
	return runtime.Session{}, fmt.Errorf("resume not supported")
}

func (p *idempotencyTestProvider) Prompt(context.Context, string, string) error { return nil }

func (p *idempotencyTestProvider) Cancel(context.Context, string) error { return nil }

func (p *idempotencyTestProvider) Events(context.Context, string) (<-chan events.Event, error) {
	return make(chan events.Event), nil
}

func (p *idempotencyTestProvider) AnswerPermission(context.Context, string, string, runtime.PermissionResponse) error {
	return nil
}

func (p *idempotencyTestProvider) Capabilities(context.Context) (runtime.Capabilities, error) {
	return runtime.Capabilities{}, nil
}

// idempotencyFailingProvider fails at session start, before any session ID is
// registered.
type idempotencyFailingProvider struct {
	idempotencyTestProvider
}

func (p *idempotencyFailingProvider) Start(context.Context, runtime.StartOptions) (runtime.Session, error) {
	return runtime.Session{}, fmt.Errorf("intentional spawn failure")
}

func newIdempotencySupervisor(t *testing.T, cfg Config, provider runtime.Provider) *Supervisor {
	t.Helper()
	cfg.ControlSocket = newStableSocketPath(t, "idempotency")
	if cfg.MaxRuntimes == 0 {
		cfg.MaxRuntimes = 8
	}
	sup := NewSupervisor(cfg)
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return provider, nil
	}
	return sup
}

func idempotencySpawnRaw(t *testing.T, key, prompt, dir string) []byte {
	t.Helper()
	raw, err := json.Marshal(SpawnParams{Prompt: prompt, Dir: dir, Agent: "claude", IdempotencyKey: key})
	if err != nil {
		t.Fatalf("marshal spawn params: %v", err)
	}
	return raw
}

func countIdempotencyRuntimes(sup *Supervisor) int {
	sup.controlMu.Lock()
	defer sup.controlMu.Unlock()
	return len(sup.runtimes)
}

func assertOneRuntime(t *testing.T, sup *Supervisor) {
	t.Helper()
	if got := countIdempotencyRuntimes(sup); got != 1 {
		t.Fatalf("runtimes = %d, want 1", got)
	}
}

// TestIdempotencySequentialIdenticalSpawnsOneRuntime: a retried spawn with the
// same key and identical parameters returns the stored result and starts no
// second runtime.
func TestIdempotencySequentialIdenticalSpawnsOneRuntime(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()
	raw := idempotencySpawnRaw(t, "key_a", "hello", dir)

	first, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	firstRes, ok := first.(SpawnResult)
	if !ok {
		t.Fatalf("first result type = %T, want SpawnResult", first)
	}
	second, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("second spawn: %v", err)
	}
	secondRes, ok := second.(SpawnResult)
	if !ok {
		t.Fatalf("second result type = %T, want SpawnResult", second)
	}
	if firstRes.RuntimeID != secondRes.RuntimeID || firstRes.SessionID != secondRes.SessionID ||
		firstRes.OnEvent != secondRes.OnEvent || firstRes.SentinelFile != secondRes.SentinelFile {
		t.Fatalf("retry did not return the stored result: first=%+v second=%+v", firstRes, secondRes)
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyConcurrentIdenticalSpawnsOneRuntime: concurrent duplicate
// spawns under one key all observe the holder's single result.
func TestIdempotencyConcurrentIdenticalSpawnsOneRuntime(t *testing.T) {
	const n = 8
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()
	raw := idempotencySpawnRaw(t, "key_a", "hello", dir)

	results := make([]SpawnResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := sup.Spawn(raw)
			errs[i] = err
			if err == nil {
				r, ok := res.(SpawnResult)
				if !ok {
					t.Errorf("spawn %d result type = %T, want SpawnResult", i, res)
					return
				}
				results[i] = r
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("spawn %d: %v", i, err)
		}
	}
	for i := 1; i < n; i++ {
		if results[i].RuntimeID != results[0].RuntimeID || results[i].SessionID != results[0].SessionID {
			t.Fatalf("spawn %d got a different runtime: %+v vs %+v", i, results[i], results[0])
		}
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyHashMismatchConflict: reusing a key with different semantic
// parameters is a conflict and starts no second runtime.
func TestIdempotencyHashMismatchConflict(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()

	if _, err := sup.Spawn(idempotencySpawnRaw(t, "key_a", "hello", dir)); err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	_, err := sup.Spawn(idempotencySpawnRaw(t, "key_a", "different prompt", dir))
	var ce *control.IdempotencyConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v, want *control.IdempotencyConflictError", err)
	}
	if ce.Key != "key_a" {
		t.Fatalf("conflict key = %q, want key_a", ce.Key)
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyExpiredTLRetrySpawnsFresh: once the stored entry's TTL has
// passed, a retry starts a fresh runtime instead of returning the stale
// result.
func TestIdempotencyExpiredTLRetrySpawnsFresh(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()
	raw := idempotencySpawnRaw(t, "key_a", "hello", dir)

	first, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	firstRes, ok := first.(SpawnResult)
	if !ok {
		t.Fatalf("first result type = %T, want SpawnResult", first)
	}

	// Age the stored entry past its TTL.
	sup.idempotency.mu.Lock()
	for key, entry := range sup.idempotency.entries {
		entry.expires = time.Now().Add(-time.Second)
		sup.idempotency.entries[key] = entry
	}
	sup.idempotency.mu.Unlock()

	second, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("retry after expiry: %v", err)
	}
	secondRes, ok := second.(SpawnResult)
	if !ok {
		t.Fatalf("second result type = %T, want SpawnResult", second)
	}
	if secondRes.RuntimeID == firstRes.RuntimeID {
		t.Fatalf("expired retry reused the stored runtime %q", firstRes.RuntimeID)
	}
	if got := countIdempotencyRuntimes(sup); got != 2 {
		t.Fatalf("runtimes = %d, want 2 after expired retry", got)
	}
	if provider.startCalls != 2 {
		t.Fatalf("provider Start calls = %d, want 2", provider.startCalls)
	}
}

// TestIdempotencyFailedSpawnNotCached: a failed spawn releases its reserved
// slot and caches nothing, so a later valid spawn under the same key starts
// fresh.
func TestIdempotencyFailedSpawnNotCached(t *testing.T) {
	sup := newIdempotencySupervisor(t, Config{}, &idempotencyFailingProvider{})
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()
	raw := idempotencySpawnRaw(t, "key_a", "hello", dir)

	if _, err := sup.Spawn(raw); err == nil {
		t.Fatal("failing spawn succeeded")
	}
	if got := countIdempotencyRuntimes(sup); got != 0 {
		t.Fatalf("runtimes = %d, want 0 after failed spawn", got)
	}
	sup.idempotency.mu.Lock()
	inflight := len(sup.idempotency.inflight)
	stored := len(sup.idempotency.entries)
	sup.idempotency.mu.Unlock()
	if inflight != 0 || stored != 0 {
		t.Fatalf("store after failure: inflight=%d stored=%d, want 0/0", inflight, stored)
	}

	provider := &idempotencyTestProvider{}
	sup.newProviderFunc = func(runtime.StartOptions, string) (runtime.Provider, error) {
		return provider, nil
	}
	res, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("valid spawn after failure: %v", err)
	}
	resRes, ok := res.(SpawnResult)
	if !ok {
		t.Fatalf("result type = %T, want SpawnResult", res)
	}
	if resRes.RuntimeID == "" {
		t.Fatal("fresh spawn returned an empty runtime ID")
	}
	assertOneRuntime(t, sup)

	// The fresh result is now cached: a third spawn is a hit.
	third, err := sup.Spawn(raw)
	if err != nil {
		t.Fatalf("third spawn: %v", err)
	}
	thirdRes, ok := third.(SpawnResult)
	if !ok {
		t.Fatalf("third result type = %T, want SpawnResult", third)
	}
	if thirdRes.RuntimeID != resRes.RuntimeID {
		t.Fatalf("third spawn = %q, want the fresh runtime %q", thirdRes.RuntimeID, resRes.RuntimeID)
	}
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyHitAtFullRuntimeCapacity: a retry of a stored key returns the
// stored result without going through admission, even when the local runtime
// capacity is full.
func TestIdempotencyHitAtFullRuntimeCapacity(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{MaxRuntimes: 1}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()

	first, err := sup.Spawn(idempotencySpawnRaw(t, "key_a", "hello", dir))
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	firstRes, ok := first.(SpawnResult)
	if !ok {
		t.Fatalf("first result type = %T, want SpawnResult", first)
	}

	// The active runtime fills the local capacity: a fresh key is rejected
	// with an admission capacity error.
	_, err = sup.Spawn(idempotencySpawnRaw(t, "key_b", "hello", dir))
	var ce *admission.CapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("fresh-key spawn error = %v, want admission capacity error", err)
	}

	// Retrying the stored key must not hit admission.
	second, err := sup.Spawn(idempotencySpawnRaw(t, "key_a", "hello", dir))
	if err != nil {
		t.Fatalf("retry of stored key: %v", err)
	}
	secondRes, ok := second.(SpawnResult)
	if !ok {
		t.Fatalf("second result type = %T, want SpawnResult", second)
	}
	if secondRes.RuntimeID != firstRes.RuntimeID {
		t.Fatalf("retry = %q, want stored runtime %q", secondRes.RuntimeID, firstRes.RuntimeID)
	}
	assertOneRuntime(t, sup)
}

// TestIdempotencyStoreFullCapacityError: a full store rejects a new key with a
// capacity error; once the occupying entry expires, the new key fits.
func TestIdempotencyStoreFullCapacityError(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{IdempotencyCapacity: 1}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()

	if _, err := sup.Spawn(idempotencySpawnRaw(t, "key_a", "hello", dir)); err != nil {
		t.Fatalf("spawn A: %v", err)
	}
	_, err := sup.Spawn(idempotencySpawnRaw(t, "key_b", "hello", dir))
	var ce *control.IdempotencyCapacityError
	if !errors.As(err, &ce) {
		t.Fatalf("spawn B error = %v, want *control.IdempotencyCapacityError", err)
	}
	if ce.Key != "key_b" || ce.Capacity != 1 {
		t.Fatalf("capacity error = %+v, want key_b capacity 1", ce)
	}

	// Expire A's entry; B now fits.
	sup.idempotency.mu.Lock()
	for key, entry := range sup.idempotency.entries {
		entry.expires = time.Now().Add(-time.Second)
		sup.idempotency.entries[key] = entry
	}
	sup.idempotency.mu.Unlock()

	res, err := sup.Spawn(idempotencySpawnRaw(t, "key_b", "hello", dir))
	if err != nil {
		t.Fatalf("spawn B after expiry: %v", err)
	}
	resRes, ok := res.(SpawnResult)
	if !ok {
		t.Fatalf("result type = %T, want SpawnResult", res)
	}
	if resRes.RuntimeID == "" {
		t.Fatal("spawn B returned an empty runtime ID")
	}
	if got := countIdempotencyRuntimes(sup); got != 2 {
		t.Fatalf("runtimes = %d, want 2", got)
	}
}

// TestIdempotencyCapacityOneConcurrentDistinctKeys: with capacity 1, two
// concurrent distinct keys admit exactly one holder; the other gets a
// capacity error.
func TestIdempotencyCapacityOneConcurrentDistinctKeys(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{IdempotencyCapacity: 1}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()
	rawA := idempotencySpawnRaw(t, "key_a", "hello", dir)
	rawB := idempotencySpawnRaw(t, "key_b", "hello", dir)

	outcomes := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := sup.Spawn(rawA)
		outcomes[0] = err
	}()
	go func() {
		defer wg.Done()
		_, err := sup.Spawn(rawB)
		outcomes[1] = err
	}()
	wg.Wait()

	var successes, capacityErrs int
	for i, err := range outcomes {
		if err == nil {
			successes++
			continue
		}
		var ce *control.IdempotencyCapacityError
		if !errors.As(err, &ce) {
			t.Fatalf("outcome %d error = %v, want *control.IdempotencyCapacityError", i, err)
		}
		capacityErrs++
	}
	if successes != 1 || capacityErrs != 1 {
		t.Fatalf("outcomes: %d success, %d capacity errors; want 1/1", successes, capacityErrs)
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyDerivedLabelsMatchAcrossRetries: a Label marked derived is
// excluded from the parameter hash, so a retry with a different derived label
// is a hit, not a conflict.
func TestIdempotencyDerivedLabelsMatchAcrossRetries(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()

	raw1, err := json.Marshal(SpawnParams{Prompt: "hello", Dir: dir, Agent: "claude", Label: "run-1", LabelDerived: true, IdempotencyKey: "key_a"})
	if err != nil {
		t.Fatalf("marshal first params: %v", err)
	}
	first, err := sup.Spawn(raw1)
	if err != nil {
		t.Fatalf("first spawn: %v", err)
	}
	firstRes, ok := first.(SpawnResult)
	if !ok {
		t.Fatalf("first result type = %T, want SpawnResult", first)
	}

	raw2, err := json.Marshal(SpawnParams{Prompt: "hello", Dir: dir, Agent: "claude", Label: "run-2", LabelDerived: true, IdempotencyKey: "key_a"})
	if err != nil {
		t.Fatalf("marshal second params: %v", err)
	}
	second, err := sup.Spawn(raw2)
	if err != nil {
		t.Fatalf("derived-label retry: %v", err)
	}
	secondRes, ok := second.(SpawnResult)
	if !ok {
		t.Fatalf("second result type = %T, want SpawnResult", second)
	}
	if secondRes.RuntimeID != firstRes.RuntimeID {
		t.Fatalf("derived-label retry = %q, want stored runtime %q", secondRes.RuntimeID, firstRes.RuntimeID)
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyExplicitLabelMismatchConflicts: an explicit (non-derived)
// Label is a semantic parameter, so a retry with a different one conflicts.
func TestIdempotencyExplicitLabelMismatchConflicts(t *testing.T) {
	provider := &idempotencyTestProvider{}
	sup := newIdempotencySupervisor(t, Config{}, provider)
	defer func() { _ = sup.broker.Stop() }()
	dir := t.TempDir()

	raw1, err := json.Marshal(SpawnParams{Prompt: "hello", Dir: dir, Agent: "claude", Label: "label-a", IdempotencyKey: "key_a"})
	if err != nil {
		t.Fatalf("marshal first params: %v", err)
	}
	if _, err := sup.Spawn(raw1); err != nil {
		t.Fatalf("first spawn: %v", err)
	}

	raw2, err := json.Marshal(SpawnParams{Prompt: "hello", Dir: dir, Agent: "claude", Label: "label-b", IdempotencyKey: "key_a"})
	if err != nil {
		t.Fatalf("marshal second params: %v", err)
	}
	_, err = sup.Spawn(raw2)
	var ce *control.IdempotencyConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v, want *control.IdempotencyConflictError", err)
	}
	assertOneRuntime(t, sup)
	if provider.startCalls != 1 {
		t.Fatalf("provider Start calls = %d, want 1", provider.startCalls)
	}
}

// TestIdempotencyWaiterReleaseOnFailure exercises the store directly: a
// waiter blocked on an in-flight key observes the holder's failure when the
// holder releases, and the commit path leaves a stored entry with no
// in-flight reservation.
func TestIdempotencyWaiterReleaseOnFailure(t *testing.T) {
	store := newIdempotencyStore(4)
	_, holder, err := store.begin("k", "h")
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	if holder == nil {
		t.Fatal("holder begin returned no flight")
	}

	waiterStarted := make(chan struct{})
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		close(waiterStarted)
		res, f, err := store.begin("k", "h")
		if f != nil {
			t.Error("waiter got a flight")
		}
		if err == nil {
			t.Error("waiter got a nil error")
		}
		if !strings.Contains(err.Error(), "intentional holder failure") {
			t.Errorf("waiter error = %v, want the holder's failure", err)
		}
		if res.RuntimeID != "" {
			t.Errorf("waiter result = %+v, want zero result on holder failure", res)
		}
	}()
	select {
	case <-waiterStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not start")
	}
	time.Sleep(50 * time.Millisecond)
	select {
	case <-waiterDone:
		t.Fatal("waiter returned before the holder settled")
	default:
	}
	store.release("k", holder, fmt.Errorf("intentional holder failure"))
	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not observe the holder's failure")
	}
	store.mu.Lock()
	inflight := len(store.inflight)
	stored := len(store.entries)
	store.mu.Unlock()
	if inflight != 0 || stored != 0 {
		t.Fatalf("store after release: inflight=%d stored=%d, want 0/0", inflight, stored)
	}

	// The commit path leaves a stored entry and drains inflight.
	_, holder2, err := store.begin("k2", "h2")
	if err != nil {
		t.Fatalf("second holder begin: %v", err)
	}
	if holder2 == nil {
		t.Fatal("second holder begin returned no flight")
	}
	store.commit("k2", holder2, SpawnResult{RuntimeID: "rt_1"})
	res, f, err := store.begin("k2", "h2")
	if err != nil || f != nil {
		t.Fatalf("retry after commit: flight=%v err=%v, want a hit", f, err)
	}
	if res.RuntimeID != "rt_1" {
		t.Fatalf("stored result = %+v, want rt_1", res)
	}
	store.mu.Lock()
	inflight = len(store.inflight)
	stored = len(store.entries)
	store.mu.Unlock()
	if inflight != 0 || stored != 1 {
		t.Fatalf("store after commit: inflight=%d stored=%d, want 0/1", inflight, stored)
	}
}

// TestIdempotencyHashIgnoresAttemptIdentity: the parameter hash is stable
// across per-attempt identity (OnEvent, SentinelFile, IdempotencyKey, derived
// Label) and changes with semantic parameters.
func TestIdempotencyHashIgnoresAttemptIdentity(t *testing.T) {
	// A derived label is excluded from the hash, so two params that differ
	// only in a derived Label (both LabelDerived) hash equal.
	base := SpawnParams{
		Prompt:         "hello",
		Dir:            "/tmp",
		Agent:          "claude",
		Label:          "run-1",
		LabelDerived:   true,
		IdempotencyKey: "key_a",
		OnEvent:        "/a",
		SentinelFile:   "/b",
	}
	baseHash, err := idempotencyHash(base)
	if err != nil {
		t.Fatalf("hash(base): %v", err)
	}

	// Per-attempt identity (OnEvent, SentinelFile, IdempotencyKey) and a
	// different derived label are all excluded.
	retry := base
	retry.OnEvent = "/a2"
	retry.SentinelFile = "/b2"
	retry.IdempotencyKey = "key_b"
	retry.Label = "run-2"
	retryHash, err := idempotencyHash(retry)
	if err != nil {
		t.Fatalf("hash(retry): %v", err)
	}
	if baseHash != retryHash {
		t.Fatalf("hash changed with per-attempt identity: %s vs %s", baseHash, retryHash)
	}

	// An explicit label is a semantic parameter.
	explicit := base
	explicit.Label = "other"
	explicit.LabelDerived = false
	explicitHash, err := idempotencyHash(explicit)
	if err != nil {
		t.Fatalf("hash(explicit): %v", err)
	}
	if baseHash == explicitHash {
		t.Fatal("hash equal for a derived vs an explicit label")
	}

	// Two different explicit labels also differ.
	explicitOther := base
	explicitOther.Label = "another"
	explicitOther.LabelDerived = false
	explicitOtherHash, err := idempotencyHash(explicitOther)
	if err != nil {
		t.Fatalf("hash(explicitOther): %v", err)
	}
	if explicitHash == explicitOtherHash {
		t.Fatal("hash equal for different explicit labels")
	}

	other := base
	other.Prompt = "goodbye"
	otherHash, err := idempotencyHash(other)
	if err != nil {
		t.Fatalf("hash(other): %v", err)
	}
	if baseHash == otherHash {
		t.Fatal("hash equal for different prompts")
	}
}

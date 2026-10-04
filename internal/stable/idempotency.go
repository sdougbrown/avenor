package stable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
)

// idempotencyTTL is how long a completed spawn result is retained for
// idempotent retries.
const idempotencyTTL = 24 * time.Hour

// defaultIdempotencyCapacity is the store capacity when Config does not set
// one.
const defaultIdempotencyCapacity = 1024

type idempotencyEntry struct {
	result  SpawnResult
	hash    string
	expires time.Time
}

// idempotencyFlight is a per-key in-flight reservation. Concurrent duplicate
// spawns wait on done and observe outcome.
type idempotencyFlight struct {
	hash   string
	done   chan struct{}
	result SpawnResult
	err    error
}

type idempotencyStore struct {
	mu       sync.Mutex
	now      func() time.Time
	capacity int
	entries  map[string]idempotencyEntry
	inflight map[string]*idempotencyFlight
	// waiterSignal, when non-nil, receives one token per waiter just before
	// it blocks on a flight's done channel, letting tests order a release
	// after waiters are parked. Production leaves it nil.
	waiterSignal chan<- struct{}
}

func newIdempotencyStore(capacity int) *idempotencyStore {
	if capacity <= 0 {
		capacity = defaultIdempotencyCapacity
	}
	return &idempotencyStore{
		now:      time.Now,
		capacity: capacity,
		entries:  map[string]idempotencyEntry{},
		inflight: map[string]*idempotencyFlight{},
	}
}

// purgeExpiredLocked deletes entries whose retention window has closed
// (expires <= now). Caller holds mu.
func (s *idempotencyStore) purgeExpiredLocked() {
	now := s.now()
	for key, entry := range s.entries {
		if !entry.expires.After(now) {
			delete(s.entries, key)
		}
	}
}

// begin gates a spawn on its idempotency key. It returns a stored result
// (nil flight, nil error) on a hit, the caller's in-flight reservation
// (non-nil flight, nil error) when the caller must perform the spawn, or a
// conflict/capacity error. A caller that finds an in-flight reservation for
// its key waits on it and returns exactly the holder's outcome.
func (s *idempotencyStore) begin(key, hash string) (SpawnResult, *idempotencyFlight, error) {
	s.mu.Lock()
	s.purgeExpiredLocked()
	if entry, ok := s.entries[key]; ok {
		if entry.hash != hash {
			s.mu.Unlock()
			return SpawnResult{}, nil, &control.IdempotencyConflictError{Key: key}
		}
		s.mu.Unlock()
		return entry.result, nil, nil
	}
	if f, ok := s.inflight[key]; ok {
		if f.hash != hash {
			s.mu.Unlock()
			return SpawnResult{}, nil, &control.IdempotencyConflictError{Key: key}
		}
		if s.waiterSignal != nil {
			s.waiterSignal <- struct{}{}
		}
		s.mu.Unlock()
		<-f.done
		return f.result, nil, f.err
	}
	if len(s.entries)+len(s.inflight) >= s.capacity {
		s.mu.Unlock()
		return SpawnResult{}, nil, &control.IdempotencyCapacityError{Key: key, Capacity: s.capacity}
	}
	f := &idempotencyFlight{hash: hash, done: make(chan struct{})}
	s.inflight[key] = f
	s.mu.Unlock()
	return SpawnResult{}, f, nil
}

// commit records the holder's successful result for the key and releases the
// in-flight reservation, waking any waiters with the result.
func (s *idempotencyStore) commit(key string, f *idempotencyFlight, result SpawnResult) {
	s.mu.Lock()
	f.result = result
	s.entries[key] = idempotencyEntry{result: result, hash: f.hash, expires: s.now().Add(idempotencyTTL)}
	if cur, ok := s.inflight[key]; ok && cur == f {
		delete(s.inflight, key)
	}
	s.mu.Unlock()
	close(f.done)
}

// release abandons the holder's in-flight reservation with an error, waking
// any waiters with the failure. Used for failure and panic paths.
func (s *idempotencyStore) release(key string, f *idempotencyFlight, err error) {
	s.mu.Lock()
	f.err = err
	if cur, ok := s.inflight[key]; ok && cur == f {
		delete(s.inflight, key)
	}
	s.mu.Unlock()
	close(f.done)
}

// IdempotencyHash derives the parameter hash for an idempotent spawn: SHA-256
// over canonical JSON of the typed params with per-attempt identity and
// supervisor-resolved provenance zeroed. OnEvent and SentinelFile are
// attempt-local artifacts; IdempotencyKey is the key itself; Label is an
// MCP-generated run ID when LabelDerived is set; ParentID is auto-populated
// from live runtime registration, SessionID is resolved from prior state on
// follow-up, ParentRunID is broker-provenance of the calling runtime, and
// AgentProfile is resolved through the profile fallback chain, so all four can
// differ between legitimate retries of the same intent.
// Everything excluded here is re-derived per attempt; caller intent stays in
// the hash so a retry with different semantic parameters still conflicts.
func IdempotencyHash(p SpawnParams) (string, error) {
	p.OnEvent = ""
	p.SentinelFile = ""
	p.IdempotencyKey = ""
	p.ParentID = ""
	p.ParentRunID = ""
	p.SessionID = ""
	p.AgentProfile = ""
	if p.LabelDerived {
		p.Label = ""
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// idempotentSpawn gates a spawn through the idempotency store. The
// store-wide mutex is never held while the runtime starts: begin either
// returns a stored hit, the caller's in-flight reservation, or an error.
func (s *Supervisor) idempotentSpawn(p SpawnParams, hash string) (SpawnResult, error) {
	key := p.IdempotencyKey
	result, flight, err := s.idempotency.begin(key, hash)
	if err != nil {
		return SpawnResult{}, err
	}
	if flight == nil {
		return result, nil
	}
	// Holder path: spawn outside the store mutex; commit on success, release
	// the reserved slot on any failure including panic.
	reserved := false
	defer func() {
		if !reserved {
			s.idempotency.release(key, flight, fmt.Errorf("spawn panicked"))
		}
	}()
	// Keyed first-use: reject a label already held by a live runtime.
	if p.Label != "" {
		s.controlMu.Lock()
		var holder string
		for _, child := range s.runtimes {
			if child.label == p.Label {
				holder = child.id
				break
			}
		}
		s.controlMu.Unlock()
		if holder != "" {
			reserved = true
			labelErr := fmt.Errorf("label already in use: %s (runtime %s)", p.Label, holder)
			s.idempotency.release(key, flight, labelErr)
			return SpawnResult{}, labelErr
		}
	}
	res, err := s.spawn(p)
	reserved = true
	if err != nil {
		s.idempotency.release(key, flight, err)
		return SpawnResult{}, err
	}
	s.idempotency.commit(key, flight, res)
	return res, nil
}

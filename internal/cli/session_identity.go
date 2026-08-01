package cli

import (
	"fmt"
	"sync"
)

// sessionBackendMap records the backend that owns each session ID observed by
// one non-stable CLI run. Authoritative IDs remain in the map for the life of
// the run; provisional IDs are aliases owned by the in-flight attempt only.
type sessionBackendMap struct {
	mu      sync.Mutex
	entries map[string]sessionBackendEntry
}

type sessionBackendEntry struct {
	backend       string
	owner         *cliSessionAttempt
	authoritative bool
}

// cliSessionAttempt is an attempt-generation token. It lets a late callback
// from an older provider fail the ownership check without relying on provider
// interface equality.
type cliSessionAttempt struct{}

func newSessionBackendMap() *sessionBackendMap {
	return &sessionBackendMap{entries: make(map[string]sessionBackendEntry)}
}

func (m *sessionBackendMap) backend(sessionID string) (string, bool) {
	if m == nil || sessionID == "" {
		return "", false
	}
	m.mu.Lock()
	entry, ok := m.entries[sessionID]
	m.mu.Unlock()
	return entry.backend, ok
}

func (m *sessionBackendMap) validateResume(sessionID, currentBackend string) error {
	if sessionID == "" || m == nil {
		return nil
	}
	if previousBackend, ok := m.backend(sessionID); ok && previousBackend != "" && previousBackend != currentBackend {
		return fmt.Errorf("cannot resume session %q with backend %q: session belongs to backend %q", sessionID, currentBackend, previousBackend)
	}
	return nil
}

// claim records the session returned by Start/Resume. An existing ID may only
// be claimed when it is the exact session being resumed; a different provider
// may not silently reuse an ID that is still or was previously owned.
func (m *sessionBackendMap) claim(sessionID, backend string, owner *cliSessionAttempt, resumeID string) error {
	if m == nil || sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]sessionBackendEntry)
	}
	if existing, ok := m.entries[sessionID]; ok {
		if existing.backend != backend {
			return fmt.Errorf("session ID %q is already assigned to backend %q, cannot use backend %q", sessionID, existing.backend, backend)
		}
		if sessionID != resumeID || existing.owner != nil {
			return fmt.Errorf("session ID %q is already owned by another attempt", sessionID)
		}
	}
	m.entries[sessionID] = sessionBackendEntry{backend: backend, owner: owner, authoritative: true}
	return nil
}

// adopt atomically moves an attempt from its provisional ID to an
// authoritative provider ID. The provisional alias is retained until finish
// so in-flight cleanup and resume checks cannot observe a gap. A provider may
// never adopt an ID already owned by another attempt.
func (m *sessionBackendMap) adopt(provisionalID, authoritativeID, backend string, owner *cliSessionAttempt) bool {
	if m == nil {
		return true
	}
	if provisionalID == "" || authoritativeID == "" || provisionalID == authoritativeID {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.entries[provisionalID]
	if !ok || old.owner != owner || old.backend != backend {
		return false
	}
	if _, exists := m.entries[authoritativeID]; exists {
		return false
	}
	old.authoritative = false
	m.entries[provisionalID] = old
	m.entries[authoritativeID] = sessionBackendEntry{backend: backend, owner: owner, authoritative: true}
	return true
}

// finish releases the in-flight owner while retaining the final authoritative
// ID. A provisional alias is removed only if it still belongs to this attempt,
// so late cleanup cannot erase a newer retry's mapping.
func (m *sessionBackendMap) finish(provisionalID, finalID, backend string, owner *cliSessionAttempt) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if finalID != "" {
		if entry, ok := m.entries[finalID]; ok && entry.owner == owner && entry.backend == backend {
			entry.owner = nil
			entry.authoritative = true
			m.entries[finalID] = entry
		}
	}
	if provisionalID != "" && provisionalID != finalID {
		if entry, ok := m.entries[provisionalID]; ok && entry.owner == owner && !entry.authoritative {
			delete(m.entries, provisionalID)
		}
	}
}

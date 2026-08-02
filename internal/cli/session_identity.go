package cli

import (
	"fmt"
	"sync"
)

// cliSessionIdentity is the complete provider identity attached to a session
// during one non-stable CLI run. Empty fields are authoritative once recorded;
// callers may omit them on resume, but may not replace them.
type cliSessionIdentity struct {
	backend      string
	agent        string
	model        string
	agentProfile string
}

// sessionBackendMap records the complete identity that owns each session ID
// observed by one non-stable CLI run. Authoritative IDs remain in the map for
// the life of the run; provisional IDs are aliases owned by an in-flight
// attempt only. The historical type name is retained to keep this local change
// isolated from callers.
type sessionBackendMap struct {
	mu      sync.Mutex
	entries map[string]sessionBackendEntry
}

type sessionBackendEntry struct {
	identity      cliSessionIdentity
	owner         *cliSessionAttempt
	authoritative bool
}

// cliSessionAttempt is an attempt-generation token. It lets a late callback
// from an older provider fail the ownership check without relying on provider
// interface equality.
type cliSessionAttempt struct {
	resumeID             string
	provisionalWasMapped bool
	rejected             bool
}

func newSessionBackendMap() *sessionBackendMap {
	return &sessionBackendMap{entries: make(map[string]sessionBackendEntry)}
}

func (m *sessionBackendMap) identity(sessionID string) (cliSessionIdentity, bool) {
	if m == nil || sessionID == "" {
		return cliSessionIdentity{}, false
	}
	m.mu.Lock()
	entry, ok := m.entries[sessionID]
	m.mu.Unlock()
	return entry.identity, ok
}

func (m *sessionBackendMap) backend(sessionID string) (string, bool) {
	identity, ok := m.identity(sessionID)
	return identity.backend, ok
}

// resolveResume restores omitted fields from the authoritative mapping and
// rejects every explicit conflict before a provider is created. Backend alone
// is not a sufficient ownership boundary: two agents or models on the same
// backend must never resume each other's conversation.
func (m *sessionBackendMap) resolveResume(sessionID string, supplied cliSessionIdentity) (cliSessionIdentity, error) {
	if sessionID == "" || m == nil {
		return supplied, nil
	}
	authoritative, ok := m.identity(sessionID)
	if !ok {
		return supplied, nil
	}
	for _, check := range []struct {
		field         string
		supplied      string
		authoritative string
	}{
		{field: "backend", supplied: supplied.backend, authoritative: authoritative.backend},
		{field: "agent", supplied: supplied.agent, authoritative: authoritative.agent},
		{field: "model", supplied: supplied.model, authoritative: authoritative.model},
		{field: "agent profile", supplied: supplied.agentProfile, authoritative: authoritative.agentProfile},
	} {
		if check.supplied != "" && check.supplied != check.authoritative {
			return cliSessionIdentity{}, fmt.Errorf("cannot resume session %q with %s %q: session belongs to %s %q", sessionID, check.field, check.supplied, check.field, check.authoritative)
		}
	}
	return authoritative, nil
}

// claim records the session returned by Start/Resume. An existing ID may only
// be claimed when it is the exact session being resumed with its complete
// mapped identity; another provider may not silently reuse an owned ID.
func (m *sessionBackendMap) claim(sessionID string, identity cliSessionIdentity, owner *cliSessionAttempt, resumeID string) error {
	if m == nil || sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[string]sessionBackendEntry)
	}
	existing, wasMapped := m.entries[sessionID]
	if wasMapped {
		if existing.identity != identity {
			return fmt.Errorf("session ID %q is already assigned to a different provider identity", sessionID)
		}
		if sessionID != resumeID || existing.owner != nil {
			return fmt.Errorf("session ID %q is already owned by another attempt", sessionID)
		}
		identity = existing.identity
	}
	if owner != nil {
		owner.resumeID = resumeID
		owner.provisionalWasMapped = wasMapped
	}
	m.entries[sessionID] = sessionBackendEntry{identity: identity, owner: owner, authoritative: true}
	return nil
}

// adopt atomically moves an attempt from its provisional ID to an
// authoritative provider ID. The provisional alias is retained until finish
// so in-flight cleanup and resume checks cannot observe a gap. A provider may
// never adopt an ID already owned by another attempt.
func (m *sessionBackendMap) adopt(provisionalID, authoritativeID string, owner *cliSessionAttempt) bool {
	if m == nil {
		return true
	}
	if provisionalID == "" || authoritativeID == "" || provisionalID == authoritativeID {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.entries[provisionalID]
	if !ok || old.owner != owner {
		return false
	}
	if existing, exists := m.entries[authoritativeID]; exists {
		if owner == nil || authoritativeID != owner.resumeID || existing.owner != nil || existing.identity != old.identity {
			if owner != nil {
				owner.rejected = true
			}
			return false
		}
	}
	old.authoritative = false
	m.entries[provisionalID] = old
	m.entries[authoritativeID] = sessionBackendEntry{identity: old.identity, owner: owner, authoritative: true}
	return true
}

// finish releases the in-flight owner while retaining the final authoritative
// ID. A provisional alias is removed only if it still belongs to this attempt,
// so late cleanup cannot erase a newer retry's mapping.
func (m *sessionBackendMap) finish(provisionalID, finalID string, owner *cliSessionAttempt) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if owner != nil && owner.rejected {
		for sessionID, entry := range m.entries {
			if entry.owner != owner {
				continue
			}
			if sessionID == provisionalID && owner.provisionalWasMapped {
				entry.owner = nil
				entry.authoritative = true
				m.entries[sessionID] = entry
				continue
			}
			delete(m.entries, sessionID)
		}
		return
	}
	if finalID != "" {
		if entry, ok := m.entries[finalID]; ok && entry.owner == owner {
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

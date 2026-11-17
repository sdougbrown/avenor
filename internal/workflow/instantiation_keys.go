package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sdougbrown/avenor/internal/durablefile"
)

// ErrInstantiationKeyConflict is the sentinel an instantiation key conflict
// reduces to; errors.Is against it works for any conflict error.
var ErrInstantiationKeyConflict = errors.New("workflow: instantiation idempotency key conflict")

// InstantiationKeyConflictError reports that a caller-supplied instantiation
// idempotency key was reused for a different request. The control server maps
// it to -32031 — the conflict code programmatic clients already classify for
// template registration and spawn idempotency — so a client can tell "pick a
// fresh key" apart from any other failure. errors.Is against
// ErrInstantiationKeyConflict holds.
type InstantiationKeyConflictError struct {
	Key             string
	TemplateID      string
	TemplateVersion string
}

func (e *InstantiationKeyConflictError) Error() string {
	return fmt.Sprintf("workflow: instantiation idempotency key conflict: key %q is bound to template %s@%s with a different request",
		e.Key, e.TemplateID, e.TemplateVersion)
}

func (e *InstantiationKeyConflictError) Is(target error) bool {
	return target == ErrInstantiationKeyConflict
}

// instantiationKeyRecord is the durable binding of one caller-supplied
// idempotency key to the workflow instance created under it. Records live at
// <root>/instantiation-keys/<sha256(key)>.json — hashed because keys are
// caller-supplied strings and must not become path components. A record is
// written before the instance is created, so a crash anywhere in between
// leaves a retry that resolves to the same workflow ID and completes the
// interrupted instantiation instead of minting a duplicate.
type instantiationKeyRecord struct {
	Key             string    `json:"key"`
	TemplateID      string    `json:"template_id"`
	TemplateVersion string    `json:"template_version"`
	RequestHash     string    `json:"request_hash"`
	WorkflowID      string    `json:"workflow_id"`
	CreatedAt       time.Time `json:"created_at"`
}

// validateInstantiationKey applies the shared durable-string rule to a
// caller-supplied key; keys land in a durable record, so the same bounds as
// instance params apply.
func validateInstantiationKey(key string) error {
	return validateDurableString("instantiation idempotency key", key)
}

// lockInstantiationKeys acquires the root-scoped flock that serializes
// instantiation key resolution across processes sharing the workflow root,
// in the templates.lock / dispatch.lock style. Callers hold it across the
// whole record-or-resume decision so a concurrent same-key retry waits and
// then observes the first attempt's outcome rather than racing it.
func (s *Store) lockInstantiationKeys() (func() error, error) {
	if err := os.MkdirAll(s.instantiationKeysDir(), 0o755); err != nil {
		return nil, err
	}
	return durablefile.Lock(filepath.Join(s.root, "instantiation-keys.lock"))
}

func (s *Store) instantiationKeysDir() string {
	return filepath.Join(s.root, "instantiation-keys")
}

func (s *Store) instantiationKeyPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(s.instantiationKeysDir(), hex.EncodeToString(sum[:])+".json")
}

// loadInstantiationKey reads the record bound to key; the second return is
// false when no record exists.
func (s *Store) loadInstantiationKey(key string) (instantiationKeyRecord, bool, error) {
	data, err := os.ReadFile(s.instantiationKeyPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return instantiationKeyRecord{}, false, nil
	}
	if err != nil {
		return instantiationKeyRecord{}, false, err
	}
	var rec instantiationKeyRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return instantiationKeyRecord{}, false, fmt.Errorf("instantiation key record: %w", err)
	}
	return rec, true, nil
}

// writeInstantiationKeyRecord durably binds rec, atomically replacing any
// prior record under the same key.
func (s *Store) writeInstantiationKeyRecord(rec instantiationKeyRecord) error {
	path := s.instantiationKeyPath(rec.Key)
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := durablefile.WriteFileAtomic(path, durablefile.TempPattern(path), data, 0o600); err != nil {
		return err
	}
	return durablefile.FsyncDir(s.instantiationKeysDir())
}

// instantiationRequestHash fingerprints the semantic content of an
// instantiate request: template, version, and params. encoding/json emits
// map keys in sorted order, so the digest is canonical across processes.
// Metadata is caller bookkeeping and deliberately outside the fingerprint:
// two requests differing only in metadata share a key without conflict.
func instantiationRequestHash(templateID TemplateID, templateVersion TemplateVersion, params map[string]string) string {
	payload := struct {
		TemplateID      TemplateID        `json:"template_id"`
		TemplateVersion TemplateVersion   `json:"template_version"`
		Params          map[string]string `json:"params,omitempty"`
	}{TemplateID: templateID, TemplateVersion: templateVersion, Params: params}
	data, err := json.Marshal(payload)
	if err != nil {
		// map[string]string marshals unconditionally; unreachable.
		panic(fmt.Sprintf("instantiation request hash: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

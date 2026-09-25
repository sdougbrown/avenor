//go:build unix

package workflowcontroller

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Limits and bounds for adapter manifests.
const (
	// AdapterMaxTimeoutMS is the largest timeout a manifest may declare.
	AdapterMaxTimeoutMS = 30000
	// AdapterMaxIOMessageBytes is the largest stdout/stderr byte limit a
	// manifest may declare (1 MiB).
	AdapterMaxIOMessageBytes = 1048576
)

// Sentinel errors returned by manifest loading and registry lookups.
var (
	// ErrAdapterManifest is returned for a manifest that fails validation.
	ErrAdapterManifest = errors.New("invalid adapter manifest")
	// ErrAdapterUntrusted is returned when a manifest or executable fails the
	// ownership or mode trust checks.
	ErrAdapterUntrusted = errors.New("adapter is not trusted")
)

// AdapterManifest is a host-owned registration of a trusted external-gate
// adapter executable. The wire fields are strict: unknown JSON fields are
// rejected on load.
type AdapterManifest struct {
	Version        int               `json:"version"`
	ID             string            `json:"id"`
	Executable     string            `json:"executable"`
	Args           []string          `json:"args"`
	TimeoutMS      int64             `json:"timeout_ms"`
	MaxStdoutBytes int64             `json:"max_stdout_bytes"`
	MaxStderrBytes int64             `json:"max_stderr_bytes"`
	InheritEnv     []string          `json:"inherit_env"`
	Inputs         map[string]string `json:"inputs"`

	// State recorded at load time and rechecked before every execution.
	manifestPath string
	manifestDir  string
	// ResolvedPath is the Executable path after symlink resolution.
	ResolvedPath string
	// Dev, Ino, Size, MtimeNS, and CtimeNS identify the resolved executable's
	// file. Size and the nanosecond timestamps accompany device and inode
	// because filesystems reuse inode numbers after a delete-and-recreate.
	Dev     uint64
	Ino     uint64
	Size    int64
	MtimeNS int64
	CtimeNS int64
	// digest is the SHA-256 of the manifest file bytes.
	digest [sha256.Size]byte
}

// AdapterLoadError records one manifest file that could not be loaded: a
// validation or trust failure, or a duplicate adapter ID shared with another
// file. Valid manifests in the same directory still load.
type AdapterLoadError struct {
	// File is the manifest filename the error is attributed to (base name
	// within the adapter directory).
	File string
	Err  error
}

// AdapterRegistry is the immutable set of loaded adapter manifests, keyed by
// adapter ID.
type AdapterRegistry struct {
	byID map[string]*AdapterManifest
	errs []AdapterLoadError
}

// Errors returns the per-file load errors recorded while building the
// registry, in directory order. Valid manifests load alongside them; only a
// failure to read the directory itself fails the whole load.
func (r *AdapterRegistry) Errors() []AdapterLoadError {
	if r == nil {
		return nil
	}
	return r.errs
}

// Lookup returns the manifest registered under id.
func (r *AdapterRegistry) Lookup(id string) (*AdapterManifest, bool) {
	if r == nil {
		return nil, false
	}
	m, ok := r.byID[id]
	return m, ok
}

// IDs returns the registered adapter IDs in sorted order.
func (r *AdapterRegistry) IDs() []string {
	ids := make([]string, 0, len(r.byID))
	for id := range r.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// LoadAdapterRegistry loads every immediate *.json file in dir as one adapter
// manifest. Subdirectories and non-.json files are ignored; the filename is
// not the adapter ID. A missing directory yields an empty registry, not an
// error. Loading is per manifest: an invalid or untrusted file is recorded as
// a load error on the returned registry while valid manifests still load;
// only a failure to read the directory itself fails the whole load. Duplicate
// adapter IDs across files reject every file declaring the ID.
func LoadAdapterRegistry(dir string) (*AdapterRegistry, error) {
	reg := &AdapterRegistry{byID: map[string]*AdapterManifest{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return reg, nil
		}
		return nil, err
	}
	// Group loaded manifests by adapter ID so duplicate IDs reject every
	// file declaring the ID instead of silently keeping one of them.
	byID := map[string][]*AdapterManifest{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		m, err := loadAdapterManifest(path)
		if err != nil {
			reg.errs = append(reg.errs, AdapterLoadError{
				File: entry.Name(),
				Err:  fmt.Errorf("adapter manifest %s: %w", entry.Name(), err),
			})
			continue
		}
		byID[m.ID] = append(byID[m.ID], m)
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ms := byID[id]
		if len(ms) == 1 {
			reg.byID[id] = ms[0]
			continue
		}
		files := make([]string, 0, len(ms))
		for _, m := range ms {
			files = append(files, filepath.Base(m.manifestPath))
		}
		reg.errs = append(reg.errs, AdapterLoadError{
			File: files[0],
			Err: fmt.Errorf("%w: adapter id %q declared by %s; all rejected",
				ErrAdapterManifest, id, strings.Join(files, ", ")),
		})
	}
	return reg, nil
}

// loadAdapterManifest reads, strictly decodes, validates, and trust-checks one
// manifest file.
func loadAdapterManifest(path string) (*AdapterManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var wire AdapterManifest
	if err := dec.Decode(&wire); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAdapterManifest, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: trailing data after JSON object", ErrAdapterManifest)
	}
	if err := validateAdapterManifest(&wire); err != nil {
		return nil, err
	}
	if err := securePath(path); err != nil {
		return nil, fmt.Errorf("%w: manifest: %v", ErrAdapterUntrusted, err)
	}
	resolved, id, err := resolveSecureExecutable(wire.Executable)
	if err != nil {
		return nil, fmt.Errorf("%w: executable: %v", ErrAdapterUntrusted, err)
	}
	wire.manifestPath = path
	wire.manifestDir = filepath.Dir(path)
	wire.ResolvedPath = resolved
	wire.Dev = id.dev
	wire.Ino = id.ino
	wire.Size = id.size
	wire.MtimeNS = id.mtimeNS
	wire.CtimeNS = id.ctimeNS
	wire.digest = sha256.Sum256(data)
	return &wire, nil
}

// validateAdapterManifest enforces the manifest wire contract.
func validateAdapterManifest(m *AdapterManifest) error {
	if m.Version != 1 {
		return fmt.Errorf("%w: version must be 1, got %d", ErrAdapterManifest, m.Version)
	}
	if err := validateAdapterID(m.ID); err != nil {
		return err
	}
	if !filepath.IsAbs(m.Executable) {
		return fmt.Errorf("%w: executable %q must be an absolute path", ErrAdapterManifest, m.Executable)
	}
	if base := filepath.Base(m.Executable); isShellInterpreter(base) && containsString(m.Args, "-c") {
		return fmt.Errorf("%w: shell interpreter %q must not be invoked with -c", ErrAdapterManifest, m.Executable)
	}
	if m.TimeoutMS < 1 || m.TimeoutMS > AdapterMaxTimeoutMS {
		return fmt.Errorf("%w: timeout_ms must be in [1, %d], got %d", ErrAdapterManifest, AdapterMaxTimeoutMS, m.TimeoutMS)
	}
	if m.MaxStdoutBytes < 1 || m.MaxStdoutBytes > AdapterMaxIOMessageBytes {
		return fmt.Errorf("%w: max_stdout_bytes must be in [1, %d], got %d", ErrAdapterManifest, AdapterMaxIOMessageBytes, m.MaxStdoutBytes)
	}
	if m.MaxStderrBytes < 1 || m.MaxStderrBytes > AdapterMaxIOMessageBytes {
		return fmt.Errorf("%w: max_stderr_bytes must be in [1, %d], got %d", ErrAdapterManifest, AdapterMaxIOMessageBytes, m.MaxStderrBytes)
	}
	for _, name := range m.InheritEnv {
		if name == "" || strings.Contains(name, "=") {
			return fmt.Errorf("%w: inherit_env entry %q must be a non-empty name without '='", ErrAdapterManifest, name)
		}
	}
	for name, typ := range m.Inputs {
		if name == "" {
			return fmt.Errorf("%w: input names must be non-empty", ErrAdapterManifest)
		}
		switch typ {
		case "string", "integer", "number", "boolean":
		default:
			return fmt.Errorf("%w: input %q has unsupported type %q", ErrAdapterManifest, name, typ)
		}
	}
	return nil
}

// validateAdapterID applies the same path-safe character rules as controller
// IDs.
func validateAdapterID(id string) error {
	if id == "" {
		return fmt.Errorf("%w: adapter id must be non-empty", ErrAdapterManifest)
	}
	if strings.HasPrefix(id, ".") {
		return fmt.Errorf("%w: adapter id %q must not start with a dot", ErrAdapterManifest, id)
	}
	for _, r := range id {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return fmt.Errorf("%w: adapter id %q contains invalid character %q", ErrAdapterManifest, id, r)
		}
	}
	return nil
}

// isShellInterpreter reports whether base names a shell or env launcher that
// could be used to run arbitrary strings.
func isShellInterpreter(base string) bool {
	switch base {
	case "sh", "bash", "zsh", "dash", "fish", "env":
		return true
	}
	return false
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

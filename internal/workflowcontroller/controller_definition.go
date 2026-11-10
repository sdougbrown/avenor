package workflowcontroller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ControllerDefinition is the host-owned declaration of one durable
// controller: its id, in-flight limit, and (optionally) desired state. A
// supervisor pointed at a definitions directory reconciles these fields into
// its controller store at startup; runtime fields (leader, lease, revision)
// stay in the store.
type ControllerDefinition struct {
	// File is the basename of the definition file the definition came from.
	File         string `json:"-"`
	ControllerID string `json:"controller_id"`
	MaxInflight  int    `json:"max_inflight"`
	// DesiredState is optional. Empty leaves the store's desired state
	// alone; "enabled" or "disabled" reconciles it.
	DesiredState DesiredState `json:"desired_state,omitempty"`
}

// ControllerDefinitionFailure records one definition file that did not load,
// alongside the definitions that did. A broken definition file must not
// disable the other controllers.
type ControllerDefinitionFailure struct {
	File string
	Err  error
}

func (f ControllerDefinitionFailure) Error() string {
	return fmt.Sprintf("%s: %v", f.File, f.Err)
}

func (f ControllerDefinitionFailure) Unwrap() error { return f.Err }

// ControllerDefinitions is the result of loading a definitions directory.
type ControllerDefinitions struct {
	Definitions []ControllerDefinition
	Failures    []ControllerDefinitionFailure
}

// LoadControllerDefinitions loads every immediate *.json file in dir as one
// controller definition. Subdirectories and non-.json files are ignored; the
// filename is not the controller ID. A missing directory yields an empty
// result, not an error. Loading is per file: an invalid definition is
// recorded as a failure while valid definitions still load; only a failure to
// read the directory itself fails the whole load. Duplicate controller IDs
// across files reject every file declaring the ID.
func LoadControllerDefinitions(dir string) (ControllerDefinitions, error) {
	loaded := ControllerDefinitions{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return loaded, nil
		}
		return loaded, err
	}
	// Group loaded definitions by controller ID so duplicate IDs reject every
	// file declaring the ID instead of silently keeping one of them.
	byID := map[string][]ControllerDefinition{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		def, err := loadControllerDefinition(path)
		if err != nil {
			loaded.Failures = append(loaded.Failures, ControllerDefinitionFailure{
				File: entry.Name(),
				Err:  fmt.Errorf("controller definition %s: %w", entry.Name(), err),
			})
			continue
		}
		byID[def.ControllerID] = append(byID[def.ControllerID], def)
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		defs := byID[id]
		if len(defs) == 1 {
			loaded.Definitions = append(loaded.Definitions, defs[0])
			continue
		}
		files := make([]string, 0, len(defs))
		for _, def := range defs {
			files = append(files, def.File)
		}
		loaded.Failures = append(loaded.Failures, ControllerDefinitionFailure{
			File: files[0],
			Err: fmt.Errorf("%w: controller id %q declared by %s; all rejected",
				ErrInvalidController, id, strings.Join(files, ", ")),
		})
	}
	return loaded, nil
}

// loadControllerDefinition reads, strictly decodes, and validates one
// definition file.
func loadControllerDefinition(path string) (ControllerDefinition, error) {
	var def ControllerDefinition
	data, err := os.ReadFile(path)
	if err != nil {
		return def, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&def); err != nil {
		return def, fmt.Errorf("parse: %w", err)
	}
	if dec.More() {
		return def, fmt.Errorf("trailing data after JSON object")
	}
	def.File = filepath.Base(path)
	if err := validateControllerID(def.ControllerID); err != nil {
		return def, err
	}
	if def.MaxInflight <= 0 {
		return def, fmt.Errorf("%w: max_inflight must be positive, got %d", ErrInvalidController, def.MaxInflight)
	}
	if def.DesiredState != "" && def.DesiredState != DesiredEnabled && def.DesiredState != DesiredDisabled {
		return def, fmt.Errorf("%w: unknown desired state %q", ErrInvalidController, def.DesiredState)
	}
	return def, nil
}

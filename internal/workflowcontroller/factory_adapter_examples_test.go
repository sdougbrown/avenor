//go:build unix

package workflowcontroller

// factory_adapter_examples_test.go audits the shipped software-factory
// adapter manifest examples (templates/software-factory/adapters/*.json):
// each strictly decodes as a manifest, passes the full wire validation, and
// matches the work template's bound review gates — the adapter IDs and the
// pinned input schema. The examples register no executable: their
// executable paths are operator placeholders, so only the wire contract is
// audited here, not trust.

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

const factoryAdapterExampleDir = "../../templates/software-factory/adapters"

func TestFactoryAdapterExamplesMatchWorkTemplateGates(t *testing.T) {
	// The work template's bound external gates pin these adapter IDs and
	// input schemas; the example manifests must declare exactly the same
	// contract.
	wantInputs := map[string]string{
		"repository":  "string",
		"pull_number": "integer",
		"head_sha":    "string",
	}
	wantAdapters := map[string]bool{
		"circleci-pipeline": true,
		"github-pr-review":  true,
	}

	entries, err := os.ReadDir(factoryAdapterExampleDir)
	if err != nil {
		t.Fatalf("read adapter examples: %v", err)
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(factoryAdapterExampleDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		var manifest AdapterManifest
		if err := dec.Decode(&manifest); err != nil {
			t.Errorf("%s: strict decode: %v", entry.Name(), err)
			continue
		}
		if err := validateAdapterManifest(&manifest); err != nil {
			t.Errorf("%s: validateAdapterManifest: %v", entry.Name(), err)
			continue
		}
		if !wantAdapters[manifest.ID] {
			t.Errorf("%s: adapter id %q is not bound by the work template", entry.Name(), manifest.ID)
		}
		if seen[manifest.ID] {
			t.Errorf("duplicate adapter id %q across example manifests", manifest.ID)
		}
		seen[manifest.ID] = true
		if !filepath.IsAbs(manifest.Executable) {
			t.Errorf("%s: executable %q must be an absolute path", entry.Name(), manifest.Executable)
		}
		// The examples must never ship a live integration: no generic CLI
		// entry points, and no inherited environment that could carry
		// credentials.
		if base := filepath.Base(manifest.Executable); base == "gh" || base == "git" {
			t.Errorf("%s: executable %q must be an operator-provided adapter, not %q", entry.Name(), manifest.Executable, base)
		}
		if len(manifest.InheritEnv) != 0 {
			t.Errorf("%s: inherit_env = %v, want none (no credentials in examples)", entry.Name(), manifest.InheritEnv)
		}
		got := map[string]string{}
		for name, typ := range manifest.Inputs {
			got[name] = typ
		}
		if len(got) != len(wantInputs) {
			t.Errorf("%s: inputs = %v, want exactly the template's pinned inputs", entry.Name(), got)
		}
		names := make([]string, 0, len(got))
		for name := range got {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if want, ok := wantInputs[name]; !ok || want != got[name] {
				t.Errorf("%s: input %q = %q, want %q (declared %v)", entry.Name(), name, got[name], want, ok)
			}
		}
	}
	for id := range wantAdapters {
		if !seen[id] {
			t.Errorf("no example manifest for adapter %q", id)
		}
	}
}

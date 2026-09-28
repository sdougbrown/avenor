//go:build unix

package workflowcontroller

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stageAdapterDir returns a fresh, owner-only adapter manifest directory.
func stageAdapterDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if resolved, err := filepath.EvalSymlinks(dir); err == nil {
		dir = resolved
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// stageFixture copies a testdata adapter script into dir with owner-only
// permissions and returns its path.
func stageFixture(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "adapters", name))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// stageScript writes an inline adapter script into dir.
func stageScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeManifestContent writes a manifest file with raw JSON content.
func writeManifestContent(t *testing.T, dir, filename, content string) string {
	t.Helper()
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// writeManifest writes a valid manifest for id pointing at exe with fixed
// args, using the shared test input schema.
func writeManifest(t *testing.T, dir, filename, id, exe string, args []string, timeoutMS int64) string {
	t.Helper()
	argsJSON, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"version":1,"id":%q,"executable":%q,"args":%s,"timeout_ms":%d,"max_stdout_bytes":65536,"max_stderr_bytes":65536,"inherit_env":["GH_SECRET"],"inputs":{"repository":"string","pull_number":"integer","head_sha":"string"}}`,
		id, exe, argsJSON, timeoutMS)
	writeManifestContent(t, dir, filename, content)
	return filepath.Join(dir, filename)
}

// loadOne loads a registry from dir and returns the manifest for id.
func loadOne(t *testing.T, dir, id string) *AdapterManifest {
	t.Helper()
	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	m, ok := reg.Lookup(id)
	if !ok {
		t.Fatalf("adapter %q not loaded", id)
	}
	return m
}

const testInputJSON = `{"repository":"sdougbrown/avenor","pull_number":143,"head_sha":"cc793f7"}`

func TestLoadAdapterRegistryValid(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)

	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	if got := reg.IDs(); len(got) != 1 || got[0] != "review" {
		t.Fatalf("IDs() = %v, want [review]", got)
	}
	m, ok := reg.Lookup("review")
	if !ok {
		t.Fatal("review not found")
	}
	if m.TimeoutMS != 5000 || m.MaxStdoutBytes != 65536 || m.MaxStderrBytes != 65536 {
		t.Fatalf("unexpected limits: %+v", m)
	}
	if m.Inputs["pull_number"] != "integer" || m.InheritEnv[0] != "GH_SECRET" {
		t.Fatalf("unexpected manifest body: %+v", m)
	}
	if m.ResolvedPath != exe {
		t.Fatalf("ResolvedPath = %q, want %q", m.ResolvedPath, exe)
	}
	if m.Dev == 0 || m.Ino == 0 {
		t.Fatal("device/inode not recorded")
	}
	if m.digest == ([32]byte{}) {
		t.Fatal("manifest digest not recorded")
	}
}

func TestLoadAdapterRegistryMissingDir(t *testing.T) {
	reg, err := LoadAdapterRegistry(filepath.Join(t.TempDir(), "absent"))
	if err != nil {
		t.Fatalf("missing dir must yield empty registry, got %v", err)
	}
	if len(reg.IDs()) != 0 {
		t.Fatalf("IDs() = %v, want empty", reg.IDs())
	}
}

func TestLoadAdapterRegistryIgnoresNonJSONAndSubdirs(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	writeManifestContent(t, dir, "notes.txt", "not a manifest")
	writeManifestContent(t, dir, "data.json.bak", "{}")
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	writeManifestContent(t, sub, "nested.json", fmt.Sprintf(`{"version":1,"id":"nested","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe))

	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	if got := reg.IDs(); len(got) != 1 || got[0] != "review" {
		t.Fatalf("IDs() = %v, want only [review]", got)
	}
}

func TestLoadAdapterRegistryDuplicateIDs(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "a.json", "dup", exe, nil, 5000)
	writeManifest(t, dir, "b.json", "dup", exe, nil, 5000)
	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	if got := reg.IDs(); len(got) != 0 {
		t.Fatalf("IDs() = %v, want empty (both duplicates rejected)", got)
	}
	errs := reg.Errors()
	if len(errs) != 1 {
		t.Fatalf("Errors() = %+v, want one duplicate-id error", errs)
	}
	if !errors.Is(errs[0].Err, ErrAdapterManifest) {
		t.Fatalf("duplicate id error = %v, want ErrAdapterManifest", errs[0].Err)
	}
	for _, name := range []string{"a.json", "b.json"} {
		if !strings.Contains(errs[0].Err.Error(), name) {
			t.Errorf("error %q does not name %s", errs[0].Err, name)
		}
	}
}

// TestLoadAdapterRegistryIsolatesBadManifest proves one invalid manifest does
// not disable its valid neighbors: the good adapter loads and the bad file is
// reported as a single per-file load error.
func TestLoadAdapterRegistryIsolatesBadManifest(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	writeManifestContent(t, dir, "broken.json", `{"version":2}`)

	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	if got := reg.IDs(); len(got) != 1 || got[0] != "review" {
		t.Fatalf("IDs() = %v, want [review] (bad file must not block good ones)", got)
	}
	m, ok := reg.Lookup("review")
	if !ok || m.TimeoutMS != 5000 {
		t.Fatalf("Lookup(review) = %+v, %v; want the loaded manifest", m, ok)
	}
	errs := reg.Errors()
	if len(errs) != 1 {
		t.Fatalf("Errors() = %+v, want one per-file error", errs)
	}
	if errs[0].File != "broken.json" {
		t.Fatalf("error file = %q, want broken.json", errs[0].File)
	}
	if !errors.Is(errs[0].Err, ErrAdapterManifest) {
		t.Fatalf("error = %v, want ErrAdapterManifest", errs[0].Err)
	}
}

// TestLoadAdapterRegistryUnusableExecutable proves an adapter whose executable
// is gone or non-executable is reported as a per-file untrusted load error
// while the registry itself still loads and the bad adapter ID is absent.
func TestLoadAdapterRegistryUnusableExecutable(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, dir string) string
	}{
		{"deleted", func(t *testing.T, dir string) string {
			exe := stageFixture(t, dir, "passed.sh")
			if err := os.Remove(exe); err != nil {
				t.Fatal(err)
			}
			return exe
		}},
		{"chmod-000", func(t *testing.T, dir string) string {
			exe := stageFixture(t, dir, "passed.sh")
			if err := os.Chmod(exe, 0); err != nil {
				t.Fatal(err)
			}
			return exe
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "chmod-000" && os.Geteuid() == 0 {
				t.Skip("chmod 000 is not meaningful as root")
			}
			dir := stageAdapterDir(t)
			exe := tc.setup(t, dir)
			writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
			reg, err := LoadAdapterRegistry(dir)
			if err != nil {
				t.Fatalf("LoadAdapterRegistry: %v (must not fail the whole load)", err)
			}
			if _, ok := reg.Lookup("review"); ok {
				t.Fatal("review must be absent (unusable executable)")
			}
			if got := reg.IDs(); len(got) != 0 {
				t.Fatalf("IDs() = %v, want empty", got)
			}
			errs := reg.Errors()
			if len(errs) != 1 {
				t.Fatalf("Errors() = %+v, want one per-file error", errs)
			}
			if errs[0].File != "review.json" {
				t.Fatalf("error file = %q, want review.json", errs[0].File)
			}
			if !errors.Is(errs[0].Err, ErrAdapterUntrusted) {
				t.Fatalf("error = %v, want ErrAdapterUntrusted", errs[0].Err)
			}
		})
	}
}

func TestLoadAdapterManifestValidation(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "unknown field",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{},"extra":true}`, exe),
			wantErr: "unknown field",
		},
		{
			name:    "bad version",
			content: fmt.Sprintf(`{"version":2,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "version must be 1",
		},
		{
			name:    "relative executable",
			content: `{"version":1,"id":"x","executable":"relative/adapter","timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`,
			wantErr: "absolute path",
		},
		{
			name:    "shell interpreter with -c",
			content: `{"version":1,"id":"x","executable":"/bin/sh","args":["-c","echo hi"],"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`,
			wantErr: "shell interpreter",
		},
		{
			name:    "bad input type",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{"a":"array"}}`, exe),
			wantErr: "unsupported type",
		},
		{
			name:    "empty inherit_env name",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[""],"inputs":{}}`, exe),
			wantErr: "inherit_env",
		},
		{
			name:    "inherit_env with =",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":["A=B"],"inputs":{}}`, exe),
			wantErr: "inherit_env",
		},
		{
			name:    "timeout too large",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":30001,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "timeout_ms",
		},
		{
			name:    "timeout zero",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":0,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "timeout_ms",
		},
		{
			name:    "stdout limit too large",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1048577,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "max_stdout_bytes",
		},
		{
			name:    "stderr limit zero",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":0,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "max_stderr_bytes",
		},
		{
			name:    "empty id",
			content: fmt.Sprintf(`{"version":1,"id":"","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "non-empty",
		},
		{
			name:    "id with slash",
			content: fmt.Sprintf(`{"version":1,"id":"a/b","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}}`, exe),
			wantErr: "invalid character",
		},
		{
			name:    "trailing data",
			content: fmt.Sprintf(`{"version":1,"id":"x","executable":%q,"timeout_ms":1000,"max_stdout_bytes":1024,"max_stderr_bytes":1024,"inherit_env":[],"inputs":{}} {}`, exe),
			wantErr: "trailing data",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := stageAdapterDir(t)
			writeManifestContent(t, sub, "m.json", tc.content)
			reg, err := LoadAdapterRegistry(sub)
			if err != nil {
				t.Fatalf("LoadAdapterRegistry: %v", err)
			}
			if got := reg.IDs(); len(got) != 0 {
				t.Fatalf("IDs() = %v, want empty", got)
			}
			errs := reg.Errors()
			if len(errs) != 1 || !errors.Is(errs[0].Err, ErrAdapterManifest) || !strings.Contains(errs[0].Err.Error(), tc.wantErr) {
				t.Fatalf("Errors() = %+v, want one ErrAdapterManifest containing %q", errs, tc.wantErr)
			}
		})
	}
}

func TestLoadAdapterRegistryRejectsGroupWritableDir(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	if err := os.Chmod(dir, 0o770); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	errs := reg.Errors()
	if len(errs) != 1 || !errors.Is(errs[0].Err, ErrAdapterUntrusted) {
		t.Fatalf("Errors() = %+v, want one ErrAdapterUntrusted", errs)
	}
}

func TestLoadAdapterRegistryRejectsWorldWritableExecutable(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	if err := os.Chmod(exe, 0o777); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	reg, err := LoadAdapterRegistry(dir)
	if err != nil {
		t.Fatalf("LoadAdapterRegistry: %v", err)
	}
	errs := reg.Errors()
	if len(errs) != 1 || !errors.Is(errs[0].Err, ErrAdapterUntrusted) {
		t.Fatalf("Errors() = %+v, want one ErrAdapterUntrusted", errs)
	}
}

func TestLoadAdapterRegistrySymlinkedExecutable(t *testing.T) {
	dir := stageAdapterDir(t)
	real := stageFixture(t, dir, "passed.sh")
	link := filepath.Join(dir, "link.sh")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, dir, "review.json", "review", link, nil, 5000)
	m := loadOne(t, dir, "review")
	if m.ResolvedPath != real {
		t.Fatalf("ResolvedPath = %q, want resolved %q", m.ResolvedPath, real)
	}
	if m.Dev == 0 || m.Ino == 0 {
		t.Fatal("device/inode not recorded for symlinked executable")
	}
}

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// buildVerifyBinary builds the avenor binary and returns its path so that
// tests can exercise the verify subcommand end-to-end.
func buildVerifyBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "avenor")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = "."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return binPath
}

func writeVerifyFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestVerifyValidLoopConfig(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"loop": [{"name":"test","prompt":"run tests"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected ok message, got: %s", out)
	}
}

func TestVerifyValidTeamConfig(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "team.json", `{
		"pre": [{"name":"setup","prompt":"init"}],
		"team": [{"name":"work","prompt":"do work"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--team-file", "team.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: team") {
		t.Fatalf("expected ok message, got: %s", out)
	}
}

func TestVerifyValidRoster(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "roster.json", `{
		"planner": {"backend": "agy", "agent": "planner"},
		"executor": {"backend": "agy", "agent": "executor"}
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--roster-file", "roster.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: roster") {
		t.Fatalf("expected ok message, got: %s", out)
	}
}

func TestVerifyRosterWithEntryLookup(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "roster.json", `{
		"planner": {"backend": "agy", "agent": "planner"}
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--roster-file", "roster.json", "--roster-entry", "planner")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "entry \"planner\" found") {
		t.Fatalf("expected entry found message, got: %s", out)
	}
}

func TestVerifyRosterEntryNotFound(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "roster.json", `{
		"planner": {"backend": "agy", "agent": "planner"}
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--roster-file", "roster.json", "--roster-entry", "missing")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
	if !contains(string(out), "entry \"missing\"") {
		t.Fatalf("expected entry error, got: %s", out)
	}
}

func TestVerifyInvalidLoopConfig(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"loop": [{"name":"test","prompt":"run tests","bogus":true}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.json")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
	if !contains(string(out), "error:") {
		t.Fatalf("expected error message, got: %s", out)
	}
}

func TestVerifyEmptyPhaseName(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"loop": [{"name":"","prompt":"no name"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.json")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
}

func TestVerifyNoArgs(t *testing.T) {
	bin := buildVerifyBinary(t)
	cmd := exec.Command(bin, "verify")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
	if !contains(string(out), "at least one of") {
		t.Fatalf("expected usage error, got: %s", out)
	}
}

func TestVerifyYAMLConfig(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "loop.yaml", "max_iterations: 3\nloop:\n  - name: test\n    prompt: run tests\n")

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.yaml")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected ok message, got: %s", out)
	}
}

func TestVerifyTOMLConfig(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()
	writeVerifyFile(t, dir, "loop.toml", "max_iterations = 3\n[[loop]]\nname = \"test\"\nprompt = \"run tests\"\n")

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.toml")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected ok message, got: %s", out)
	}
}

func TestVerifyNestedLoopFile(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	// Outer loop has a phase that delegates to a nested loop file.
	writeVerifyFile(t, dir, "outer.json", `{
		"max_iterations": 1,
		"loop": [{"name":"delegate","loop_file":"inner.json"}]
	}`)
	writeVerifyFile(t, dir, "inner.json", `{
		"max_iterations": 2,
		"loop": [{"name":"inner-work","prompt":"do inner work"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "outer.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected ok messages, got: %s", out)
	}
	// Should have validated both the outer and inner loop
	outerCount := containsCount(string(out), "ok: loop")
	if outerCount < 2 {
		t.Fatalf("expected at least 2 ok: loop messages (outer + nested), got %d: %s", outerCount, out)
	}
}

func TestVerifyNestedLoopFileInvalid(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	// Outer loop delegates to a nested loop file that has an invalid config.
	writeVerifyFile(t, dir, "outer.json", `{
		"max_iterations": 1,
		"loop": [{"name":"delegate","loop_file":"inner.json"}]
	}`)
	writeVerifyFile(t, dir, "inner.json", `{
		"max_iterations": 2,
		"loop": [{"name":"inner-work","prompt":"do work","bogus":true}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "outer.json")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for invalid nested config, got nil\n%s", out)
	}
	if !contains(string(out), "error:") {
		t.Fatalf("expected error message for nested config, got: %s", out)
	}
}

func TestVerifyLoopWithRosterAndRosterEntryRef(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	writeVerifyFile(t, dir, "roster.json", `{
		"reviewer": {"backend": "agy", "agent": "reviewer"}
	}`)
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"roster_file": "roster.json",
		"loop": [{"name":"review","prompt":"review code","roster_entry":"reviewer"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
}

func TestVerifyLoopWithMissingRosterEntryRef(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	writeVerifyFile(t, dir, "roster.json", `{
		"reviewer": {"backend": "agy", "agent": "reviewer"}
	}`)
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"roster_file": "roster.json",
		"loop": [{"name":"review","prompt":"review code","roster_entry":"missing-entry"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "loop.json")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit for missing roster entry, got nil\n%s", out)
	}
	if !contains(string(out), "missing-entry") {
		t.Fatalf("expected missing-entry error, got: %s", out)
	}
}

func TestVerifyFileNotFound(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	cmd := exec.Command(bin, "verify", "--dir", dir, "--loop-file", "nonexistent.json")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
	if !contains(string(out), "error:") {
		t.Fatalf("expected error message, got: %s", out)
	}
}

// helpers

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func containsCount(s, substr string) int {
	count := 0
	for {
		idx := indexOf(s, substr)
		if idx < 0 {
			break
		}
		count++
		s = s[idx+len(substr):]
	}
	return count
}
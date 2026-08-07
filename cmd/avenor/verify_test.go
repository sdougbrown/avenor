package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var (
	buildOnce   sync.Once
	builtBinPath string
	buildErr    error
)

// buildVerifyBinary builds the avenor binary once and caches the path so
// that all verify tests share a single compilation. The binary is placed
// in a process-level temp directory (not t.TempDir) so it survives across
// test functions.
func buildVerifyBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "avenor-verify-test-")
		if err != nil {
			buildErr = fmt.Errorf("create temp dir: %w", err)
			return
		}
		builtBinPath = filepath.Join(dir, "avenor")
		cmd := exec.Command("go", "build", "-o", builtBinPath, ".")
		cmd.Dir = "."
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return builtBinPath
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
	if !contains(string(out), "name must not be empty") {
		t.Fatalf("expected name-empty error, got: %s", out)
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
	// Both the outer and inner loop should be validated — assert on file paths
	// rather than just counting occurrences.
	if !contains(string(out), "outer.json") {
		t.Fatalf("expected outer.json in output, got: %s", out)
	}
	if !contains(string(out), "inner.json") {
		t.Fatalf("expected inner.json in output, got: %s", out)
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
	if !contains(string(out), "inner.json") {
		t.Fatalf("expected inner.json in error output, got: %s", out)
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
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected ok: loop message, got: %s", out)
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
	if !contains(string(out), "nonexistent.json") {
		t.Fatalf("expected nonexistent.json in error, got: %s", out)
	}
}

func TestVerifyRosterEntryWithoutRosterFile(t *testing.T) {
	bin := buildVerifyBinary(t)
	cmd := exec.Command(bin, "verify", "--roster-entry", "planner")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit, got nil\n%s", out)
	}
	if !contains(string(out), "--roster-entry requires --roster-file") {
		t.Fatalf("expected usage error, got: %s", out)
	}
}

func TestVerifyNestedTeamFile(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	// Outer team has a phase that delegates to a nested team file.
	writeVerifyFile(t, dir, "outer.json", `{
		"team": [{"name":"delegate","team_file":"inner.json"}]
	}`)
	writeVerifyFile(t, dir, "inner.json", `{
		"team": [{"name":"inner-work","prompt":"do inner work"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir, "--team-file", "outer.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "outer.json") {
		t.Fatalf("expected outer.json in output, got: %s", out)
	}
	if !contains(string(out), "inner.json") {
		t.Fatalf("expected inner.json in output, got: %s", out)
	}
}

func TestVerifyCombinedLoopTeamRoster(t *testing.T) {
	bin := buildVerifyBinary(t)
	dir := t.TempDir()

	writeVerifyFile(t, dir, "roster.json", `{
		"reviewer": {"backend": "agy", "agent": "reviewer"}
	}`)
	writeVerifyFile(t, dir, "loop.json", `{
		"max_iterations": 3,
		"loop": [{"name":"test","prompt":"run tests"}]
	}`)
	writeVerifyFile(t, dir, "team.json", `{
		"team": [{"name":"work","prompt":"do work"}]
	}`)

	cmd := exec.Command(bin, "verify", "--dir", dir,
		"--loop-file", "loop.json", "--team-file", "team.json", "--roster-file", "roster.json")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out)
	}
	if !contains(string(out), "ok: loop") {
		t.Fatalf("expected loop ok, got: %s", out)
	}
	if !contains(string(out), "ok: team") {
		t.Fatalf("expected team ok, got: %s", out)
	}
	if !contains(string(out), "ok: roster") {
		t.Fatalf("expected roster ok, got: %s", out)
	}
}

// helpers

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
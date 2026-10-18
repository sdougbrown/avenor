package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeReqFile writes a minimal .req file with the given options to dir/<name>.req
// and returns the perm-base path (without the .req suffix).
func writeReqFile(t *testing.T, dir, name string, optionIDs []string) string {
	t.Helper()
	type reqOption struct {
		OptionID string `json:"optionId"`
		Kind     string `json:"kind"`
	}
	type reqBody struct {
		RequestID string      `json:"request_id"`
		Question  string      `json:"question"`
		Options   []reqOption `json:"options"`
	}
	opts := make([]reqOption, len(optionIDs))
	for i, id := range optionIDs {
		opts[i] = reqOption{OptionID: id, Kind: id}
	}
	body := reqBody{RequestID: "1", Question: "Proceed?", Options: opts}
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	permBase := filepath.Join(dir, name)
	if err := os.WriteFile(permBase+".req", data, 0o644); err != nil {
		t.Fatalf("write req: %v", err)
	}
	return permBase
}

// writeReqFileRaw writes a raw JSON body to dir/<name>.req and returns the perm-base.
func writeReqFileRaw(t *testing.T, dir, name string, body []byte) string {
	t.Helper()
	permBase := filepath.Join(dir, name)
	if err := os.WriteFile(permBase+".req", body, 0o644); err != nil {
		t.Fatalf("write req: %v", err)
	}
	return permBase
}

// answerRun calls runAnswerTo and captures stderr.
func answerRun(args []string) (code int, stderr string) {
	var buf bytes.Buffer
	code = runAnswerTo(args, &buf)
	return code, buf.String()
}

// noTmpFiles asserts no .tmp* files exist in dir.
func noTmpFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp") {
			t.Errorf("unexpected tmp file left in dir: %s", e.Name())
		}
	}
}

// ---- table-driven tests ----

func TestRunAnswer(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, dir string) []string // returns args
		wantCode  int
		stderrSub string // non-empty: stderr must contain this
	}{
		{
			name: "missing req file exits 2 with message",
			setup: func(t *testing.T, dir string) []string {
				return []string{"--option", "allow", filepath.Join(dir, "no-such.perm")}
			},
			wantCode:  2,
			stderrSub: "does not exist",
		},
		{
			name: "malformed req JSON exits 2",
			setup: func(t *testing.T, dir string) []string {
				p := filepath.Join(dir, "bad.perm")
				if err := os.WriteFile(p+".req", []byte("{not json}"), 0o644); err != nil {
					t.Fatalf("setup: %v", err)
				}
				return []string{"--option", "allow", p}
			},
			wantCode:  2,
			stderrSub: "parse",
		},
		{
			name: "missing --option exits 2",
			setup: func(t *testing.T, dir string) []string {
				permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})
				return []string{permBase}
			},
			wantCode:  2,
			stderrSub: "--option",
		},
		{
			// Stronger assertion is in TestRunAnswerUnknownOptionListsValid;
			// just pin the exit code here.
			name: "unknown option id exits 2",
			setup: func(t *testing.T, dir string) []string {
				permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})
				return []string{"--option", "bogus", permBase}
			},
			wantCode: 2,
		},
		{
			name: "response exists without --force exits 2",
			setup: func(t *testing.T, dir string) []string {
				permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
				responsePath := permBase + ".req.response"
				if err := os.WriteFile(responsePath, []byte(`{"outcome":"selected","option_id":"allow","message":""}`+"\n"), 0o600); err != nil {
					t.Fatalf("setup: %v", err)
				}
				return []string{"--option", "allow", permBase}
			},
			wantCode:  2,
			stderrSub: "--force",
		},
		{
			name: "bogus outcome exits 2",
			setup: func(t *testing.T, dir string) []string {
				permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
				return []string{"--option", "allow", "--outcome", "bogus", permBase}
			},
			wantCode:  2,
			stderrSub: "outcome",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			args := tt.setup(t, dir)
			code, stderr := answerRun(args)

			if code != tt.wantCode {
				t.Errorf("exit code = %d, want %d; stderr: %s", code, tt.wantCode, stderr)
			}
			if tt.stderrSub != "" && !containsStr(stderr, tt.stderrSub) {
				t.Errorf("stderr %q does not contain %q", stderr, tt.stderrSub)
			}
		})
	}
}

// ---- flag position tests (path first, flags first, mixed) ----

// TestRunAnswerFlagPositionVariants: flags must be accepted both before and
// after the <perm-base> positional, since the flag package stops at the first
// positional but callers may use either order.
func TestRunAnswerFlagPositionVariants(t *testing.T) {
	tests := []struct {
		name      string
		args      func(permBase string) []string
		wantCode  int
		stderrSub string // non-empty: stderr must contain this
		wantOpt   string // when wantCode == 0, expected option_id in the response
		wantMsg   string // when wantCode == 0, expected message in the response
	}{
		{
			name: "flags after path",
			args: func(p string) []string {
				return []string{p, "--option", "allow", "--message", "after path"}
			},
			wantCode: 0,
			wantOpt:  "allow",
			wantMsg:  "after path",
		},
		{
			name: "flags before path",
			args: func(p string) []string {
				return []string{"--option", "allow", "--message", "before path", p}
			},
			wantCode: 0,
			wantOpt:  "allow",
			wantMsg:  "before path",
		},
		{
			name: "mixed flag positions",
			args: func(p string) []string {
				return []string{"--message", "mixed", p, "--option", "allow"}
			},
			wantCode: 0,
			wantOpt:  "allow",
			wantMsg:  "mixed",
		},
		{
			name: "missing --option with path first",
			args: func(p string) []string {
				return []string{p}
			},
			wantCode:  2,
			stderrSub: "--option",
		},
		{
			// origin/main also reports this message here: the second pass stops
			// at the extra positional, so a --option that follows it is never
			// parsed and the missing --option error wins.
			name: "two positionals",
			args: func(p string) []string {
				return []string{p, filepath.Join("dir", "other.perm"), "--option", "allow"}
			},
			wantCode:  2,
			stderrSub: "--option <id> is required",
		},
		{
			name: "extra positional after flags before path",
			args: func(p string) []string {
				return []string{"--option", "allow", p, filepath.Join("dir", "other.perm")}
			},
			wantCode:  2,
			stderrSub: "expected exactly one",
		},
		{
			name: "unknown flag after path",
			args: func(p string) []string {
				return []string{p, "--bogus", "x"}
			},
			wantCode:  2,
			stderrSub: "flag provided but not defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})
			code, stderr := answerRun(tt.args(permBase))

			if code != tt.wantCode {
				t.Fatalf("exit code = %d, want %d; stderr: %s", code, tt.wantCode, stderr)
			}
			if tt.stderrSub != "" && !containsStr(stderr, tt.stderrSub) {
				t.Errorf("stderr %q does not contain %q", stderr, tt.stderrSub)
			}
			if tt.wantCode != 0 {
				return
			}

			data, err := os.ReadFile(permBase + ".req.response")
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			var resp answerResponse
			if err := json.Unmarshal(data, &resp); err != nil {
				t.Fatalf("parse response: %v", err)
			}
			if resp.OptionID != tt.wantOpt {
				t.Errorf("option_id = %q, want %q", resp.OptionID, tt.wantOpt)
			}
			if resp.Message != tt.wantMsg {
				t.Errorf("message = %q, want %q", resp.Message, tt.wantMsg)
			}
			noTmpFiles(t, dir)
		})
	}
}

// ---- focused tests for response file content ----

func TestRunAnswerResponseContent(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})

	code, stderr := answerRun([]string{"--option", "allow", "--message", "looks good", permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr)
	}

	responsePath := permBase + ".req.response"
	data, err := os.ReadFile(responsePath)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var resp answerResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Outcome != "selected" {
		t.Errorf("outcome = %q, want %q", resp.Outcome, "selected")
	}
	if resp.OptionID != "allow" {
		t.Errorf("option_id = %q, want %q", resp.OptionID, "allow")
	}
	if resp.Message != "looks good" {
		t.Errorf("message = %q, want %q", resp.Message, "looks good")
	}
}

func TestRunAnswerResponseAllFieldsPresent(t *testing.T) {
	// Even with no --message, all three fields must be present in the JSON.
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"deny"})

	code, stderr := answerRun([]string{"--option", "deny", permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr)
	}

	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	for _, key := range []string{"outcome", "option_id", "message"} {
		if _, ok := raw[key]; !ok {
			t.Errorf("response JSON missing field %q; got: %s", key, strings.TrimSpace(string(data)))
		}
	}
}

func TestRunAnswerCancelledOutcome(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	code, stderr := answerRun([]string{"--option", "allow", "--outcome", "cancelled", permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr)
	}

	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp answerResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Outcome != "cancelled" {
		t.Errorf("outcome = %q, want %q", resp.Outcome, "cancelled")
	}
}

func TestRunAnswerForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	responsePath := permBase + ".req.response"

	// Write first response.
	code, stderr := answerRun([]string{"--option", "allow", "--message", "first", permBase})
	if code != 0 {
		t.Fatalf("first write exit code = %d, stderr: %s", code, stderr)
	}

	// Overwrite with --force.
	code, stderr = answerRun([]string{"--option", "allow", "--message", "second", "--force", permBase})
	if code != 0 {
		t.Fatalf("force overwrite exit code = %d, stderr: %s", code, stderr)
	}

	data, err := os.ReadFile(responsePath)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp answerResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Message != "second" {
		t.Errorf("message = %q, want %q (force overwrite did not take effect)", resp.Message, "second")
	}
}

// TestRunAnswerUsageMatchesOriginMain: the usage output for unknown flags (and
// -h) must render exactly as origin/main prints it: plain string placeholders
// for the three string flags, no placeholder for -force, and the
// (default "selected") note on -outcome. Wrapping flag values in custom Value
// types would change both UnquoteUsage and isZeroValue output.
func TestRunAnswerUsageMatchesOriginMain(t *testing.T) {
	code, stderr := answerRun([]string{"--bogus", "x"})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	wantLines := []string{
		"Usage of avenor answer:",
		"  -force\n    \toverwrite an existing response file\n",
		"  -message string\n    \tfree-text message to include in the response\n",
		"  -option string\n    \toption id to select (required)\n",
		"  -outcome string\n    \toutcome: selected or cancelled (default \"selected\")\n",
		"(default \"selected\")",
	}
	for _, want := range wantLines {
		if !containsStr(stderr, want) {
			t.Errorf("usage output missing %q; got:\n%s", want, stderr)
		}
	}
	// UnquoteUsage must render "string" placeholders, not the generic "value"
	// that a custom Value type would produce.
	for _, bad := range []string{"-message value", "-option value", "-outcome value", "-force value"} {
		if containsStr(stderr, bad) {
			t.Errorf("usage output contains %q; origin/main renders string placeholders instead:\n%s", bad, stderr)
		}
	}
}

// TestRunAnswerForceAfterPathOverwrites mirrors TestRunAnswerForceOverwrites
// with the flags placed after the <perm-base> positional.
func TestRunAnswerForceAfterPathOverwrites(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	responsePath := permBase + ".req.response"

	// Write first response.
	code, stderr := answerRun([]string{permBase, "--option", "allow", "--message", "first"})
	if code != 0 {
		t.Fatalf("first write exit code = %d, stderr: %s", code, stderr)
	}

	// Overwrite with --force after the path.
	code, stderr = answerRun([]string{permBase, "--force", "--option", "allow", "--message", "second"})
	if code != 0 {
		t.Fatalf("force overwrite exit code = %d, stderr: %s", code, stderr)
	}

	data, err := os.ReadFile(responsePath)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp answerResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Message != "second" {
		t.Errorf("message = %q, want %q (force overwrite did not take effect)", resp.Message, "second")
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerDuplicateAfterPathLastWins: the same flag given twice in the
// single pass that follows the path is not a duplicate-across-passes error;
// the flag package applies last-wins.
func TestRunAnswerDuplicateAfterPathLastWins(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})

	code, stderr := answerRun([]string{permBase, "--option", "allow", "--option", "deny"})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}
	if containsStr(stderr, "given more than once") {
		t.Errorf("repeating a flag within the same pass must not be a duplicate error: %q", stderr)
	}

	var resp answerResponse
	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.OptionID != "deny" {
		t.Errorf("option_id = %q, want %q (last value must win)", resp.OptionID, "deny")
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerTerminatorExtraPositional: after a "--" terminator every token
// is a positional. With --option supplied the extra positional is the reported
// error; without it the missing --option error wins (origin/main priority).
func TestRunAnswerTerminatorExtraPositional(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	other := filepath.Join(dir, "other.perm")

	code, stderr := answerRun([]string{"--option", "allow", "--", permBase, other})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "expected exactly one") {
		t.Errorf("stderr %q does not contain the extra-positional message", stderr)
	}
	if _, err := os.Stat(permBase + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("response file should not have been written; stat err: %v", err)
	}

	code, stderr = answerRun([]string{"--", permBase, other})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "--option <id> is required") {
		t.Errorf("stderr %q does not contain the missing --option message", stderr)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerMessageAfterPath: --message after the path consumes the next
// token as its value even when that token looks like a <perm-base> path, and
// the response is written for the positional that started the argument list.
func TestRunAnswerMessageAfterPath(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	lookalike := filepath.Join(dir, "other.perm")

	code, stderr := answerRun([]string{permBase, "--message", lookalike, "--option", "allow"})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}

	var resp answerResponse
	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Message != lookalike {
		t.Errorf("message = %q, want %q", resp.Message, lookalike)
	}
	if resp.OptionID != "allow" {
		t.Errorf("option_id = %q, want %q", resp.OptionID, "allow")
	}
	// The look-alike path must not have gained its own response file.
	if _, err := os.Stat(lookalike + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("look-alike path should not have a response file; stat err: %v", err)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerEmptyPathPositional: an explicitly supplied empty-string path
// behaves as on origin/main: the positional check passes and the read of
// ".req" fails with the does-not-exist message.
func TestRunAnswerEmptyPathPositional(t *testing.T) {
	code, stderr := answerRun([]string{"--option", "allow", ""})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "request file .req does not exist") {
		t.Errorf("stderr %q does not contain the missing-request-file message", stderr)
	}
}

func TestRunAnswerUnknownOptionListsValid(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny", "ignore"})

	code, stderr := answerRun([]string{"--option", "bogus", permBase})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	// All three valid IDs must appear in the error message.
	for _, id := range []string{"allow", "deny", "ignore"} {
		if !containsStr(stderr, id) {
			t.Errorf("stderr %q does not mention valid option %q", stderr, id)
		}
	}
}

// ---- edge case tests ----

// TestRunAnswerEmptyOptionsArray: request has "options": []. Any --option value
// should be rejected with exit 2. Error message must not have a dangling colon.
func TestRunAnswerEmptyOptionsArray(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFileRaw(t, dir, "test.perm",
		[]byte(`{"request_id":"1","question":"Go?","options":[]}`))

	code, stderr := answerRun([]string{"--option", "allow", permBase})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	// Must not emit "valid options: " with a trailing empty list.
	if containsStr(stderr, "valid options: \n") || containsStr(stderr, "valid options:  ") {
		t.Errorf("stderr has dangling 'valid options:' with empty list: %q", stderr)
	}
	// Must mention that no options were offered.
	if !containsStr(stderr, "no options were offered") {
		t.Errorf("stderr %q should mention no options were offered", stderr)
	}
}

// TestRunAnswerOptionsMissingEntirely: request JSON has no "options" field.
// Same expectation as empty options array.
func TestRunAnswerOptionsMissingEntirely(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFileRaw(t, dir, "test.perm",
		[]byte(`{"request_id":"1","question":"Go?"}`))

	code, stderr := answerRun([]string{"--option", "allow", permBase})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "no options were offered") {
		t.Errorf("stderr %q should mention no options were offered", stderr)
	}
}

// TestRunAnswerOptionMissingOptionId: option entry has no optionId field.
// Such entries must be filtered out (not treated as a valid empty-string option).
func TestRunAnswerOptionMissingOptionId(t *testing.T) {
	dir := t.TempDir()
	// One option with no optionId, so the effective valid set is empty.
	permBase := writeReqFileRaw(t, dir, "test.perm",
		[]byte(`{"request_id":"1","question":"Go?","options":[{"kind":"allow"}]}`))

	// Attempting to pass --option "" should be rejected (empty string is filtered out).
	code, stderr := answerRun([]string{"--option", "", permBase})
	// --option "" triggers the "required" check before we even read the req file.
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}

	// Attempting to pass a non-empty option should also be rejected since
	// the only entry had an empty optionId (filtered) → effective set is empty.
	permBase2 := writeReqFileRaw(t, dir, "test2.perm",
		[]byte(`{"request_id":"1","question":"Go?","options":[{"kind":"allow"}]}`))
	code2, stderr2 := answerRun([]string{"--option", "allow", permBase2})
	if code2 != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code2, stderr2)
	}
	if !containsStr(stderr2, "no options were offered") {
		t.Errorf("stderr %q should mention no options were offered", stderr2)
	}
}

// TestRunAnswerRenameFailureCleansTmp: make the rename fail by pre-creating a
// directory at the response path (with --force so the existence check passes,
// then the rename fails because target is a directory). Assert exit 1 and no
// orphaned .tmp* files.
func TestRunAnswerRenameFailureCleansTmp(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	responsePath := permBase + ".req.response"

	// Pre-create a directory at the response path so os.Rename will fail.
	if err := os.Mkdir(responsePath, 0o755); err != nil {
		t.Fatalf("setup mkdir: %v", err)
	}

	code, _ := answerRun([]string{"--option", "allow", "--force", permBase})
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (I/O error)", code)
	}

	// No orphaned .tmp* files should remain.
	noTmpFiles(t, dir)
}

// TestRunAnswerMessageSpecialChars: round-trip a message with special characters
// to verify JSON encoding works and HTML escaping is disabled.
func TestRunAnswerMessageSpecialChars(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	specialMsg := "newline:\nquote:\"backslash:\\amp:&lt:<gt:>non-ascii:é"

	code, stderr := answerRun([]string{"--option", "allow", "--message", specialMsg, permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr: %s", code, stderr)
	}

	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	var resp answerResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Message != specialMsg {
		t.Errorf("message round-trip failed:\n got:  %q\n want: %q", resp.Message, specialMsg)
	}

	// With HTML escaping disabled, the raw file must NOT contain the unicode-escape
	// sequences that the default json.Marshal would emit for <, >, & chars.
	raw := string(data)
	for _, escaped := range []string{"\\u003c", "\\u003e", "\\u0026"} {
		if containsStr(raw, escaped) {
			t.Errorf("response file contains HTML unicode-escape %q; SetEscapeHTML(false) should prevent this: %s", escaped, raw)
		}
	}
}

// TestRunAnswerNoForceLeavesExistingUntouched: verifies that an existing response
// is left byte-for-byte intact when --force is not passed, and no tmp files linger.
func TestRunAnswerNoForceLeavesExistingUntouched(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	responsePath := permBase + ".req.response"

	// Write a sentinel that is deliberately different from what `answer` would write.
	sentinel := []byte("SENTINEL_BYTES_DO_NOT_OVERWRITE\n")
	if err := os.WriteFile(responsePath, sentinel, 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}

	code, stderr := answerRun([]string{"--option", "allow", permBase})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}

	// Response file must be byte-equal to sentinel.
	got, err := os.ReadFile(responsePath)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Errorf("response file was modified; got %q, want sentinel %q", got, sentinel)
	}

	// No tmp files must exist.
	noTmpFiles(t, dir)
}

// ---- flag-order edge cases (flags after the positional, "--" terminator) ----

// TestRunAnswerDuplicateFlagAcrossPath: a flag set before <perm-base> and set
// again after it is a usage error, and no response may be written.
func TestRunAnswerDuplicateFlagAcrossPath(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow", "deny"})

	code, stderr := answerRun([]string{"--option", "first", permBase, "--option", "second"})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "avenor answer: flag -option given more than once") {
		t.Errorf("stderr %q does not contain duplicate-flag message", stderr)
	}
	if _, err := os.Stat(permBase + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("response file should not have been written; stat err: %v", err)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerDuplicateOutcomeAcrossPath: same duplicate rule for --outcome.
func TestRunAnswerDuplicateOutcomeAcrossPath(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	code, stderr := answerRun([]string{"--outcome", "deny", permBase, "--outcome", "selected"})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "avenor answer: flag -outcome given more than once") {
		t.Errorf("stderr %q does not contain duplicate-flag message", stderr)
	}
	if _, err := os.Stat(permBase + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("response file should not have been written; stat err: %v", err)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerTerminatorBeforePath: flags before a "--" terminator are parsed,
// and the path after the terminator is the positional.
func TestRunAnswerTerminatorBeforePath(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	code, stderr := answerRun([]string{"--option", "allow", "--", permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}

	var resp answerResponse
	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.OptionID != "allow" {
		t.Errorf("option_id = %q, want %q", resp.OptionID, "allow")
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerTerminatorGuardsFollowingFlags: with "--" before the path, the
// tokens after the path are positionals, not flags -- so "--force" after the
// path is an extra positional, and no response is written.
func TestRunAnswerTerminatorGuardsFollowingFlags(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	code, stderr := answerRun([]string{"--option", "allow", "--", permBase, "--force"})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "expected exactly one") {
		t.Errorf("stderr %q does not contain extra-positional message", stderr)
	}
	if _, err := os.Stat(permBase + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("response file should not have been written; stat err: %v", err)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerTrailingTerminator: a "--" after the flags that follow the path
// ends an empty flag section and succeeds.
func TestRunAnswerTrailingTerminator(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})

	code, stderr := answerRun([]string{permBase, "--option", "allow", "--"})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}

	var resp answerResponse
	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.OptionID != "allow" {
		t.Errorf("option_id = %q, want %q", resp.OptionID, "allow")
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerMessageValueThatLooksLikePath: a flag value that happens to look
// like a <perm-base> path must be consumed as the flag's value, not lifted as
// the positional.
func TestRunAnswerMessageValueThatLooksLikePath(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	lookalike := filepath.Join(dir, "other.perm")

	code, stderr := answerRun([]string{"--message", lookalike, "--option", "allow", permBase})
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr)
	}

	var resp answerResponse
	data, err := os.ReadFile(permBase + ".req.response")
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp.Message != lookalike {
		t.Errorf("message = %q, want %q", resp.Message, lookalike)
	}
	if resp.OptionID != "allow" {
		t.Errorf("option_id = %q, want %q", resp.OptionID, "allow")
	}
	// The look-alike path must not have gained its own response file.
	if _, err := os.Stat(lookalike + ".req.response"); !os.IsNotExist(err) {
		t.Errorf("look-alike path should not have a response file; stat err: %v", err)
	}
	noTmpFiles(t, dir)
}

// TestRunAnswerTwoPositionalsErrorPriority: with two positionals and no
// --option, origin/main reports the missing --option before the extra
// positional; pin that priority.
func TestRunAnswerTwoPositionalsErrorPriority(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	other := filepath.Join(dir, "other.perm")

	code, stderr := answerRun([]string{permBase, other})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "--option <id> is required") {
		t.Errorf("stderr %q does not contain the missing --option message", stderr)
	}
	if containsStr(stderr, "expected exactly one") {
		t.Errorf("extra-positional message must not win over the missing --option message: %q", stderr)
	}
}

// TestRunAnswerBogusOutcomeBeatsExtraPositional: with an invalid --outcome and
// an extra positional, origin/main reports the outcome error first; pin that
// priority too.
func TestRunAnswerBogusOutcomeBeatsExtraPositional(t *testing.T) {
	dir := t.TempDir()
	permBase := writeReqFile(t, dir, "test.perm", []string{"allow"})
	other := filepath.Join(dir, "other.perm")

	code, stderr := answerRun([]string{"--outcome", "bogus", permBase, other})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "--outcome must be") {
		t.Errorf("stderr %q does not contain the outcome error", stderr)
	}
	if containsStr(stderr, "expected exactly one") {
		t.Errorf("extra-positional message must not win over the outcome error: %q", stderr)
	}

	// Flags after an extra positional are not parsed: the second parse pass
	// stops at the first positional, so this input reports the missing --option
	// exactly as origin/main does.
	code, stderr = answerRun([]string{permBase, other, "--option", "allow"})
	if code != 2 {
		t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr)
	}
	if !containsStr(stderr, "--option <id> is required") {
		t.Errorf("stderr %q does not contain the missing --option message", stderr)
	}
	if containsStr(stderr, "expected exactly one") {
		t.Errorf("extra-positional message must not win over the missing --option message: %q", stderr)
	}
}

//go:build unix

package workflowcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testRequest(input string) PollRequest {
	return PollRequest{
		Version:      1,
		PollID:       "poll_1",
		WorkflowID:   "wf_1",
		NodeID:       "publication",
		ActivationID: "act_1",
		GateID:       "external-review",
		Input:        json.RawMessage(input),
	}
}

// invokeFixture loads a registry containing one adapter named id running the
// named fixture script, then invokes it with the standard test input.
func invokeFixture(t *testing.T, fixture, id string, input string) (AdapterResult, error) {
	t.Helper()
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, fixture)
	writeManifest(t, dir, id+".json", id, exe, nil, 5000)
	m := loadOne(t, dir, id)
	return Invoke(context.Background(), m, testRequest(input))
}

func TestInvokeTypedInputReachesAdapter(t *testing.T) {
	res, err := invokeFixture(t, "echo-input.sh", "echo", testInputJSON)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if res.Result != AdapterResultPending {
		t.Fatalf("result = %q, want pending", res.Result)
	}
	for _, want := range []string{`"repository":"sdougbrown/avenor"`, `"pull_number":143`, `"head_sha":"cc793f7"`} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary %q does not contain %s", res.Summary, want)
		}
	}
}

func TestInvokeRejectsInvalidInputBeforeExec(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"missing input", `{"repository":"sdougbrown/avenor","pull_number":143}`, "missing input"},
		{"extra input", `{"repository":"r","pull_number":1,"head_sha":"h","extra":true}`, "unexpected input"},
		{"mistyped input", `{"repository":"r","pull_number":"143","head_sha":"h"}`, "must be an integer"},
		{"non-integer number", `{"repository":"r","pull_number":1.5,"head_sha":"h"}`, "integral"},
		{"non-object input", `[1,2]`, "JSON object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := stageAdapterDir(t)
			exe := stageFixture(t, dir, "echo-input.sh")
			writeManifest(t, dir, "echo.json", "echo", exe, nil, 5000)
			m := loadOne(t, dir, "echo")
			_, err := Invoke(context.Background(), m, testRequest(tc.input))
			if err == nil || !errors.Is(err, ErrAdapterInvalidInput) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want ErrAdapterInvalidInput containing %q", err, tc.wantErr)
			}
			if _, err := os.Stat(filepath.Join(dir, "input.json")); !os.IsNotExist(err) {
				t.Fatal("adapter ran despite invalid input")
			}
		})
	}
}

func TestInvokeIntegerBounds(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "echo-input.sh")
	writeManifest(t, dir, "echo.json", "echo", exe, nil, 5000)
	m := loadOne(t, dir, "echo")

	ok := fmt.Sprintf(`{"repository":"r","pull_number":%d,"head_sha":"h"}`, int64(1)<<53)
	if _, err := Invoke(context.Background(), m, testRequest(ok)); err != nil {
		t.Fatalf("2^53 should be accepted: %v", err)
	}
	tooBig := fmt.Sprintf(`{"repository":"r","pull_number":%d,"head_sha":"h"}`, int64(1)<<54)
	_, err := Invoke(context.Background(), m, testRequest(tooBig))
	if err == nil || !errors.Is(err, ErrAdapterInvalidInput) {
		t.Fatalf("error = %v, want ErrAdapterInvalidInput", err)
	}
}

func TestInvokeDetectsReplacedExecutable(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	m := loadOne(t, dir, "review")

	if err := os.Remove(exe); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("#!/bin/sh\ncat > /dev/null\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := Invoke(context.Background(), m, testRequest(testInputJSON))
	if !errors.Is(err, ErrAdapterChanged) {
		t.Fatalf("error = %v, want ErrAdapterChanged", err)
	}
}

func TestInvokeDetectsEditedManifest(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "passed.sh")
	path := writeManifest(t, dir, "review.json", "review", exe, nil, 5000)
	m := loadOne(t, dir, "review")

	if err := os.WriteFile(path, []byte(fmt.Sprintf(`{"version":1,"id":"review","executable":%q,"timeout_ms":5001,"max_stdout_bytes":65536,"max_stderr_bytes":65536,"inherit_env":["GH_SECRET"],"inputs":{}}`, exe)), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Invoke(context.Background(), m, testRequest(testInputJSON))
	if !errors.Is(err, ErrAdapterChanged) {
		t.Fatalf("error = %v, want ErrAdapterChanged", err)
	}
}

func TestInvokeTimeoutKillsProcessGroup(t *testing.T) {
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "hang.sh")
	writeManifest(t, dir, "hang.json", "hang", exe, nil, 500)
	m := loadOne(t, dir, "hang")

	if _, err := Invoke(context.Background(), m, testRequest(testInputJSON)); !errors.Is(err, ErrAdapterTimeout) {
		t.Fatalf("error = %v, want ErrAdapterTimeout", err)
	}
	pidData, err := os.ReadFile(filepath.Join(dir, "child.pid"))
	if err != nil {
		t.Fatalf("child pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatalf("bad pid %q: %v", pidData, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child sleeper %d still alive after group kill", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestInvokeStdoutBound(t *testing.T) {
	_, err := invokeFixture(t, "huge-stdout.sh", "huge", testInputJSON)
	if !errors.Is(err, ErrAdapterOutputTooLarge) || !strings.Contains(err.Error(), "stdout") {
		t.Fatalf("error = %v, want ErrAdapterOutputTooLarge for stdout", err)
	}
}

func TestInvokeStderrBound(t *testing.T) {
	_, err := invokeFixture(t, "huge-stderr.sh", "huge", testInputJSON)
	if !errors.Is(err, ErrAdapterOutputTooLarge) || !strings.Contains(err.Error(), "stderr") {
		t.Fatalf("error = %v, want ErrAdapterOutputTooLarge for stderr", err)
	}
}

func TestInvokeRedactsStderr(t *testing.T) {
	t.Setenv("GH_SECRET", "supersecret-value-42")
	res, err := invokeFixture(t, "stderr-secret.sh", "secret", testInputJSON)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !strings.Contains(res.Stderr, "[REDACTED]") {
		t.Fatalf("stderr %q lacks [REDACTED]", res.Stderr)
	}
	if strings.Contains(res.Stderr, "supersecret-value-42") {
		t.Fatalf("stderr %q leaks the secret", res.Stderr)
	}
}

func TestInvokeEnvOnlyAllowlisted(t *testing.T) {
	t.Setenv("GH_TOKEN", "token-value-1")
	t.Setenv("ALLOWED_ONE", "allowed-value-1")
	dir := stageAdapterDir(t)
	exe := stageFixture(t, dir, "env-dump.sh")
	content := fmt.Sprintf(`{"version":1,"id":"envy","executable":%q,"args":[],"timeout_ms":5000,"max_stdout_bytes":65536,"max_stderr_bytes":65536,"inherit_env":["GH_TOKEN","ALLOWED_ONE"],"inputs":{}}`, exe)
	writeManifestContent(t, dir, "envy.json", content)
	m := loadOne(t, dir, "envy")

	res, err := Invoke(context.Background(), m, testRequest(`{}`))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !strings.Contains(res.Summary, "GH_TOKEN=token-value-1") || !strings.Contains(res.Summary, "ALLOWED_ONE=allowed-value-1") {
		t.Fatalf("summary %q lacks allowlisted vars", res.Summary)
	}
	for _, banned := range []string{"PATH=", "HOME=", "TMPDIR="} {
		if strings.Contains(res.Summary, banned) {
			t.Errorf("summary %q leaks %s", res.Summary, banned)
		}
	}
}

func TestInvokeParsesResultKinds(t *testing.T) {
	cases := []struct {
		fixture string
		want    string
	}{
		{"passed.sh", AdapterResultPassed},
		{"pending.sh", AdapterResultPending},
		{"changes-requested.sh", AdapterResultChangesRequested},
		{"empty-commented.sh", AdapterResultPending},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			res, err := invokeFixture(t, tc.fixture, "kind", testInputJSON)
			if err != nil {
				t.Fatalf("Invoke: %v", err)
			}
			if res.Result != tc.want {
				t.Fatalf("result = %q, want %q", res.Result, tc.want)
			}
			if res.Subject.Repository != "sdougbrown/avenor" || res.Subject.PullRequest != 143 || res.Subject.Revision != "cc793f7" || res.Subject.Type != "pull_request" {
				t.Fatalf("unexpected subject: %+v", res.Subject)
			}
			if res.ObservedAt.IsZero() {
				t.Fatal("observed_at missing")
			}
		})
	}

	dir := stageAdapterDir(t)
	for id, verdict := range map[string]string{
		"failed":          AdapterResultFailed,
		"action_required": AdapterResultActionRequired,
	} {
		exe := stageScript(t, dir, id+".sh", fmt.Sprintf(`cat > /dev/null
printf '%%s' '{"version":1,"result":%q,"subject":{"type":"pull_request","repository":"sdougbrown/avenor","pull_request":143,"revision":"cc793f7"},"observed_at":"2026-01-01T00:00:00Z","summary":"s"}'`, verdict))
		writeManifest(t, dir, id+".json", id, exe, nil, 5000)
		m := loadOne(t, dir, id)
		res, err := Invoke(context.Background(), m, testRequest(testInputJSON))
		if err != nil {
			t.Fatalf("Invoke %s: %v", id, err)
		}
		if res.Result != verdict {
			t.Fatalf("result = %q, want %q", res.Result, verdict)
		}
	}
}

func TestInvokePendingRetryAfter(t *testing.T) {
	res, err := invokeFixture(t, "pending.sh", "pending", testInputJSON)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if res.RetryAfterMS == nil || *res.RetryAfterMS != 45000 {
		t.Fatalf("retry_after_ms = %v, want 45000", res.RetryAfterMS)
	}
	if got := ClampRetryDelay(time.Duration(*res.RetryAfterMS) * time.Millisecond); got != 45*time.Second {
		t.Fatalf("ClampRetryDelay = %v, want 45s", got)
	}
}

func TestInvokeRejectsBadResults(t *testing.T) {
	dir := stageAdapterDir(t)
	subject := `{"type":"pull_request","repository":"r","pull_request":1,"revision":"x"}`
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"silent", "", ""},
		{"trailing-data", fmt.Sprintf(`{"version":1,"result":"pending","subject":%s,"observed_at":"2026-01-01T00:00:00Z","summary":"s"} trailing`, subject), "trailing data"},
		{"unknown-field", fmt.Sprintf(`{"version":1,"result":"pending","subject":%s,"observed_at":"2026-01-01T00:00:00Z","summary":"s","extra":1}`, subject), "unknown field"},
		{"bad-verdict", fmt.Sprintf(`{"version":1,"result":"approved","subject":%s,"observed_at":"2026-01-01T00:00:00Z","summary":"s"}`, subject), "unknown result"},
		{"missing-subject", `{"version":1,"result":"pending","observed_at":"2026-01-01T00:00:00Z","summary":"s"}`, "subject is required"},
		{"bad-timestamp", fmt.Sprintf(`{"version":1,"result":"pending","subject":%s,"observed_at":"not-a-time","summary":"s"}`, subject), "invalid result"},
		{"negative-retry", fmt.Sprintf(`{"version":1,"result":"pending","subject":%s,"observed_at":"2026-01-01T00:00:00Z","summary":"s","retry_after_ms":-5}`, subject), "non-negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exe := stageScript(t, dir, tc.name+".sh", fmt.Sprintf("cat > /dev/null\nprintf '%%s' '%s'\n", tc.body))
			writeManifest(t, dir, tc.name+".json", tc.name, exe, nil, 5000)
			m := loadOne(t, dir, tc.name)
			_, err := Invoke(context.Background(), m, testRequest(testInputJSON))
			if err == nil || !errors.Is(err, ErrAdapterInvalidResult) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want ErrAdapterInvalidResult containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestInvokeResponseHash(t *testing.T) {
	res, err := invokeFixture(t, "passed.sh", "hash", testInputJSON)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	sum := sha256.Sum256(res.RawStdout)
	if res.ResponseHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("ResponseHash = %s, want sha256 of exact stdout bytes", res.ResponseHash)
	}
	if !strings.Contains(string(res.RawStdout), `"result":"passed"`) {
		t.Fatalf("raw stdout %q is not the adapter response", res.RawStdout)
	}
}

func TestClampRetryDelay(t *testing.T) {
	cases := []struct {
		in, want time.Duration
	}{
		{0, 30 * time.Second},
		{10 * time.Second, 30 * time.Second},
		{45 * time.Second, 45 * time.Second},
		{5 * time.Minute, 5 * time.Minute},
		{time.Hour, 5 * time.Minute},
	}
	for _, tc := range cases {
		if got := ClampRetryDelay(tc.in); got != tc.want {
			t.Errorf("ClampRetryDelay(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

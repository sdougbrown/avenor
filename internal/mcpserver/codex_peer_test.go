package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

const testCodexSession = "01a1-test-session"

func writeCodexPeerRecord(t *testing.T, brokerURL, runID, token string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "peers", "codex", testCodexSession)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw, err := json.Marshal(codexPeerRecord{
		Version:   1,
		SessionID: testCodexSession,
		BrokerURL: brokerURL,
		RunID:     runID,
		RunToken:  token,
		Label:     "codex-a",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	file := filepath.Join(dir, "identity.json")
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}
	return file
}

func TestReadCodexPeerRecordRejectsUnsafeShapes(t *testing.T) {
	good := writeCodexPeerRecord(t, "http://127.0.0.1:9", "rt_codex", "tok")
	if peer, err := readCodexPeerRecord(good); err != nil || peer.record.RunID != "rt_codex" {
		t.Fatalf("valid record: peer=%+v err=%v", peer, err)
	}

	if err := os.Chmod(good, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := readCodexPeerRecord(good); err == nil {
		t.Fatal("a world-readable record was accepted")
	}

	sessionMismatch := filepath.Join(t.TempDir(), "peers", "codex", "other-session")
	if err := os.MkdirAll(sessionMismatch, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	raw, _ := json.Marshal(codexPeerRecord{
		Version: 1, SessionID: testCodexSession, BrokerURL: "http://127.0.0.1:9",
		RunID: "rt", RunToken: "tok",
	})
	file := filepath.Join(sessionMismatch, "identity.json")
	if err := os.WriteFile(file, raw, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := readCodexPeerRecord(file); err == nil {
		t.Fatal("a record naming another session was accepted")
	}
}

func TestSplitCodexPeerFile(t *testing.T) {
	raw := json.RawMessage(`{"action":"tell","target":"claude-a","message":"hi","codex_peer_file":"/tmp/x"}`)
	cleaned, file, err := splitCodexPeerFile(raw)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if file != "/tmp/x" {
		t.Fatalf("file = %q", file)
	}
	var fields map[string]any
	if err := json.Unmarshal(cleaned, &fields); err != nil {
		t.Fatalf("decode cleaned: %v", err)
	}
	if _, ok := fields["codex_peer_file"]; ok {
		t.Fatal("binding survived into the router input")
	}
	if fields["target"] != "claude-a" {
		t.Fatalf("other arguments changed: %v", fields)
	}

	if _, file, err := splitCodexPeerFile(json.RawMessage(`{"action":"list"}`)); err != nil || file != "" {
		t.Fatalf("absent binding: file=%q err=%v", file, err)
	}
	if _, _, err := splitCodexPeerFile(json.RawMessage(`{"action":"list","codex_peer_file":123}`)); err == nil {
		t.Fatal("a non-string binding was accepted")
	}
}

func TestPeerSurfaceSpeaksAsTheCodexSession(t *testing.T) {
	var gotRunID, gotToken string
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sessions" {
			t.Errorf("path = %q, want /sessions", r.URL.Path)
		}
		gotRunID = r.Header.Get(messaging.HeaderRunID)
		gotToken = r.Header.Get(messaging.HeaderRunToken)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"messaging_protocol":1,"generation":"g1","sessions":[`+
			`{"run_id":"rt_codex","label":"codex-a"},{"run_id":"rt_claude","label":"claude-b"}]}`)
	}))
	defer broker.Close()

	file := writeCodexPeerRecord(t, broker.URL, "rt_codex", "tok_secret")
	server := &Server{}
	surface, err := server.messageSurfaceFor(context.Background(), json.RawMessage(`{"action":"list"}`), file, true)
	if err != nil {
		t.Fatalf("messageSurfaceFor: %v", err)
	}
	defer surface.cleanup()

	result, err := router.ExecutePeers(context.Background(), surface.provider, json.RawMessage(`{"action":"list"}`))
	if err != nil {
		t.Fatalf("ExecutePeers: %v", err)
	}
	if len(result.Peers) != 2 {
		t.Fatalf("peers = %d, want 2", len(result.Peers))
	}
	if gotRunID != "rt_codex" {
		t.Fatalf("broker saw run %q, want the Codex session's own", gotRunID)
	}
	if gotToken != "tok_secret" {
		t.Fatalf("broker saw token %q, want the Codex session's own", gotToken)
	}
}

func TestMessageSurfaceRejectsAStaleCodexRecord(t *testing.T) {
	server := &Server{}
	if _, err := server.messageSurfaceFor(context.Background(), json.RawMessage(`{"action":"list"}`), "/tmp/peers/codex/x/identity.json", true); err == nil {
		t.Fatal("a missing record was accepted")
	}
}

func TestAskClaimIsASingleHandoff(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "asks")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "av1.ask.json"), []byte(`{"message_id":"av1.ask","asker_run_id":"rt_asker"}`), 0o600); err != nil {
		t.Fatalf("write ask: %v", err)
	}
	provider := &codexPeerProvider{asksDir: dir}

	asker, ok := provider.ClaimAsk("av1.ask")
	if !ok || asker != "rt_asker" {
		t.Fatalf("claim = (%q,%v), want (rt_asker,true)", asker, ok)
	}
	if _, ok := provider.ClaimAsk("av1.ask"); ok {
		t.Fatal("one ask was claimed twice")
	}
	provider.RestoreAsk("av1.ask", "rt_asker")
	if asker, ok := provider.ClaimAsk("av1.ask"); !ok || asker != "rt_asker" {
		t.Fatalf("after restore claim = (%q,%v), want (rt_asker,true)", asker, ok)
	}
}

func TestResumeIgnoresACodexPeerBinding(t *testing.T) {
	file := writeCodexPeerRecord(t, "http://127.0.0.1:9", "rt_codex", "tok")
	server := &Server{
		registry: NewRunRegistry(),
		opts: Options{
			SupervisorSocket: filepath.Join(t.TempDir(), "missing.sock"),
			NoAutostart:      true,
		},
	}
	raw := json.RawMessage(`{"action":"resume","target":"x","message":"y"}`)
	if _, err := server.messageSurfaceFor(context.Background(), raw, file, false); err == nil {
		t.Fatal("resume used the peer binding")
	} else if strings.Contains(err.Error(), "peer record") {
		t.Fatalf("resume read the peer record: %v", err)
	}
}

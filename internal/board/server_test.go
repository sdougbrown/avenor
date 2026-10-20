package board

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sdougbrown/avenor/internal/control"
	"github.com/sdougbrown/avenor/internal/control/controltest"
)

// controlStub records the owner-gated calls a board mutation reaches.
type controlStub struct {
	control.StableHandler
	cancelled []string
	prompted  []string
}

func (s *controlStub) RuntimeCancel(runtimeID string) error {
	s.cancelled = append(s.cancelled, runtimeID)
	return nil
}

func (s *controlStub) RuntimePrompt(runtimeID, text, _ string) error {
	s.prompted = append(s.prompted, runtimeID+":"+text)
	return nil
}

// startControlSocket starts a control server that published its owner record
// under dir and returns the absolute socket path.
func startControlSocket(t *testing.T, dir, name string) (string, *controlStub) {
	t.Helper()
	stub := &controlStub{}
	srv, _ := controltest.NewServer(control.NewState("run_1", "", 0))
	srv.SetStableHandler(stub)
	socket := filepath.Join(dir, name)
	if err := srv.Start(socket); err != nil {
		t.Fatalf("start control server: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return socket, stub
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "brd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func startBoard(t *testing.T, cfg Config) *Server {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := s.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Stop(ctx)
	})
	return s
}

func postMutation(t *testing.T, s *Server, path string, body any, token string) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+s.Addr()+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// TestBoardMutationAllowlist proves a mutation reaches the configured owner
// socket through the explicit owner attach.
func TestBoardMutationAllowlist(t *testing.T) {
	dir := shortTempDir(t)
	socket, stub := startControlSocket(t, dir, "configured.sock")
	s := startBoard(t, Config{Socket: socket, AllowMutations: true, Token: "t0ken"})

	if resp := postMutation(t, s, "/api/cancel", map[string]string{"socket": socket, "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel: %d, want 200", resp.StatusCode)
	}
	if resp := postMutation(t, s, "/api/prompt", map[string]string{"socket": socket, "text": "hi", "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusOK {
		t.Fatalf("prompt: %d, want 200", resp.StatusCode)
	}
	if len(stub.cancelled) != 1 || stub.cancelled[0] != "rt_1" {
		t.Fatalf("cancelled = %v, want [rt_1]", stub.cancelled)
	}
	if len(stub.prompted) != 1 || stub.prompted[0] != "rt_1:hi" {
		t.Fatalf("prompted = %v, want [rt_1:hi]", stub.prompted)
	}
}

// TestBoardUnlistedTargetRejected proves a socket enumerated from the scan
// directories is allowlisted, while a live socket the operator's configuration
// did not select is refused with 403 and never reached.
func TestBoardUnlistedTargetRejected(t *testing.T) {
	dir := shortTempDir(t)
	socket, stub := startControlSocket(t, dir, "listed.sock")
	unlisted, unlistedStub := startControlSocket(t, dir, "unlisted.sock")
	s := startBoard(t, Config{ScanDirs: []string{dir}, AllowMutations: true, Token: "t0ken"})

	if resp := postMutation(t, s, "/api/cancel", map[string]string{"socket": socket, "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusOK {
		t.Fatalf("enumerated cancel: %d, want 200", resp.StatusCode)
	}
	if len(stub.cancelled) != 1 {
		t.Fatalf("cancelled = %v, want one call", stub.cancelled)
	}

	// Allowlist the listed socket only; the second socket is live but unlisted.
	s2 := startBoard(t, Config{Socket: socket, AllowMutations: true, Token: "t0ken"})
	if resp := postMutation(t, s2, "/api/cancel", map[string]string{"socket": unlisted, "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted cancel: %d, want 403", resp.StatusCode)
	}
	if resp := postMutation(t, s2, "/api/prompt", map[string]string{"socket": unlisted, "text": "hi"}, "t0ken"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unlisted prompt: %d, want 403", resp.StatusCode)
	}
	if len(unlistedStub.cancelled) != 0 || len(unlistedStub.prompted) != 0 {
		t.Fatalf("unlisted socket reached: %v %v", unlistedStub.cancelled, unlistedStub.prompted)
	}
}

// TestBoardNoAuthMutationUnchanged proves the board's own authentication scope
// did not change: a no-auth board without a token still mutates.
func TestBoardNoAuthMutationUnchanged(t *testing.T) {
	dir := shortTempDir(t)
	socket, stub := startControlSocket(t, dir, "noauth.sock")
	s := startBoard(t, Config{Socket: socket, AllowMutations: true, NoAuth: true})

	cfgResp, err := http.Get("http://" + s.Addr() + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer cfgResp.Body.Close()
	var cfg map[string]any
	if err := json.NewDecoder(cfgResp.Body).Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["mutations_allowed"] != true {
		t.Fatalf("mutations_allowed = %v, want true", cfg["mutations_allowed"])
	}
	if cfg["auth_required"] != false {
		t.Fatalf("auth_required = %v, want false", cfg["auth_required"])
	}

	if resp := postMutation(t, s, "/api/cancel", map[string]string{"socket": socket, "runtime_id": "rt_1"}, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("no-auth cancel: %d, want 200", resp.StatusCode)
	}
	if len(stub.cancelled) != 1 {
		t.Fatalf("cancelled = %v, want one call", stub.cancelled)
	}
}

// TestBoardSymlinkedTargetRejected proves a symlinked socket path is refused
// instead of being followed to the allowlisted socket.
func TestBoardSymlinkedTargetRejected(t *testing.T) {
	dir := shortTempDir(t)
	socket, stub := startControlSocket(t, dir, "real.sock")
	link := filepath.Join(dir, "link.sock")
	if err := os.Symlink(socket, link); err != nil {
		t.Fatal(err)
	}
	s := startBoard(t, Config{Socket: socket, AllowMutations: true, Token: "t0ken"})

	if resp := postMutation(t, s, "/api/cancel", map[string]string{"socket": link, "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("symlinked cancel: %d, want 403", resp.StatusCode)
	}
	if len(stub.cancelled) != 0 {
		t.Fatalf("symlinked target reached the socket: %v", stub.cancelled)
	}

	// A symlink is not followed even when trusted configuration named it.
	configured := startBoard(t, Config{Socket: link, AllowMutations: true, Token: "t0ken"})
	if resp := postMutation(t, configured, "/api/cancel", map[string]string{"socket": link, "runtime_id": "rt_1"}, "t0ken"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("configured symlink cancel: %d, want 403", resp.StatusCode)
	}
	if len(stub.cancelled) != 0 {
		t.Fatalf("configured symlink reached the socket: %v", stub.cancelled)
	}
}

// TestBoardReadHandlersStayCredentialFree proves the read-only handlers still
// work with no owner record beside the socket.
func TestBoardReadHandlersStayCredentialFree(t *testing.T) {
	dir := shortTempDir(t)
	socket, _ := startControlSocket(t, dir, "read.sock")
	if err := os.Remove(control.OwnerRecordPath(socket)); err != nil {
		t.Fatal(err)
	}
	s := startBoard(t, Config{Socket: socket, Token: "t0ken"})

	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/api/status?socket="+socket, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer t0ken")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}
}

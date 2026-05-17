package mcpserver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/client"
)

type fakeClient struct {
	listResult   []map[string]any
	statusResult map[string]any
	listErr      error
	statusErr    error
}

func (f *fakeClient) Status(runtimeID string) (map[string]any, error) {
	return f.statusResult, f.statusErr
}

func (f *fakeClient) List() ([]map[string]any, error) {
	return f.listResult, f.listErr
}

func (f *fakeClient) Close() error {
	return nil
}

func TestNewServerInvalidOptions(t *testing.T) {
	t.Run("empty transport", func(t *testing.T) {
		_, err := NewServer(Options{})
		if err == nil {
			t.Fatal("expected error for empty transport")
		}
		if !strings.Contains(err.Error(), "transport is required") {
			t.Fatalf("expected error to mention transport is required, got: %v", err)
		}
	})

	t.Run("no-autostart without supervisor socket", func(t *testing.T) {
		_, err := NewServer(Options{
			Transport:   "stdio",
			NoAutostart: true,
		})
		if err == nil {
			t.Fatal("expected error for no-autostart without supervisor socket")
		}
		if !strings.Contains(err.Error(), "no-autostart requires") {
			t.Fatalf("expected error to mention no-autostart requires, got: %v", err)
		}
	})

	t.Run("invalid transport", func(t *testing.T) {
		_, err := NewServer(Options{
			Transport: "invalid",
		})
		if err == nil {
			t.Fatal("expected error for invalid transport")
		}
		if !strings.Contains(err.Error(), "unsupported transport") {
			t.Fatalf("expected error to mention unsupported transport, got: %v", err)
		}
	})
}

func TestAvenorStatusList(t *testing.T) {
	fake := &fakeClient{
		listResult: []map[string]any{
			{"id": "run1", "status": "running"},
			{"id": "run2", "status": "stopped"},
		},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", result)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 results, got %d", len(list))
	}
	if list[0]["id"] != "run1" || list[0]["status"] != "running" {
		t.Fatalf("expected {id: run1, status: running}, got %v", list[0])
	}
	if list[1]["id"] != "run2" || list[1]["status"] != "stopped" {
		t.Fatalf("expected {id: run2, status: stopped}, got %v", list[1])
	}
}

func TestAvenorStatusSingle(t *testing.T) {
	fake := &fakeClient{
		statusResult: map[string]any{"id": "run1", "status": "running"},
	}
	s, err := NewServer(Options{
		Transport:     "stdio",
		ControlClient: fake,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run1"})
	if err != nil {
		t.Fatal(err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["id"] != "run1" {
		t.Fatalf("expected id=run1, got %v", m["id"])
	}
	if m["status"] != "running" {
		t.Fatalf("expected status=running, got %v", m["status"])
	}
}

func TestAvenorStatusError(t *testing.T) {
	t.Run("list error", func(t *testing.T) {
		fake := &fakeClient{
			listErr: fmt.Errorf("connection refused"),
		}
		s, err := NewServer(Options{
			Transport:     "stdio",
			ControlClient: fake,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
		if err == nil {
			t.Fatal("expected error from list")
		}
		if !strings.Contains(err.Error(), "list runs") || !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("expected error to contain 'list runs: connection refused', got: %v", err)
		}
	})

	t.Run("status error", func(t *testing.T) {
		fake := &fakeClient{
			statusErr: fmt.Errorf("runtime not found"),
		}
		s, err := NewServer(Options{
			Transport:     "stdio",
			ControlClient: fake,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "missing"})
		if err == nil {
			t.Fatal("expected error from status")
		}
		if !strings.Contains(err.Error(), "status:") || !strings.Contains(err.Error(), "runtime not found") {
			t.Fatalf("expected error to contain 'status: runtime not found', got: %v", err)
		}
	})
}

func TestAvenorStatusNilClient(t *testing.T) {
	s, err := NewServer(Options{
		Transport:     "stdio",
		ControlClient: nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err == nil {
		t.Fatal("expected error for nil control client")
	}
	if !strings.Contains(err.Error(), "control client not available") {
		t.Fatalf("expected error to contain 'control client not available', got: %v", err)
	}
}

func startFakeSupervisor(t *testing.T) (string, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					var req client.Request
					if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
						continue
					}
					var resp client.Response
					switch req.Method {
					case "status":
						resp = client.Response{JSONRPC: "2.0", ID: req.ID}
						snap := map[string]any{"session_id": "ses_test", "phase": "working"}
						resp.Result, _ = json.Marshal(snap)
					case "list":
						resp = client.Response{JSONRPC: "2.0", ID: req.ID}
						list := []map[string]any{{"runtime_id": "rt_1", "status": "running"}}
						resp.Result, _ = json.Marshal(list)
					default:
						resp = client.Response{JSONRPC: "2.0", ID: req.ID, Error: &client.RespError{Code: -32601, Message: "method not found"}}
					}
					data, _ := json.Marshal(resp)
					data = append(data, '\n')
					c.Write(data)
				}
			}(conn)
		}
	}()

	return path, func() { ln.Close() }
}

func TestServerWithRealSocketStatus(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run1"})
	if err != nil {
		t.Fatalf("handleAvenorStatus: %v", err)
	}
	m, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}
	if m["session_id"] != "ses_test" {
		t.Errorf("session_id = %v, want ses_test", m["session_id"])
	}
	if m["phase"] != "working" {
		t.Errorf("phase = %v, want working", m["phase"])
	}
}

func TestServerWithRealSocketList(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer s.Close()

	_, result, err := s.handleAvenorStatus(context.Background(), nil, statusArgs{})
	if err != nil {
		t.Fatalf("handleAvenorStatus: %v", err)
	}
	list, ok := result.([]map[string]any)
	if !ok {
		t.Fatalf("expected []map[string]any, got %T", result)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 result, got %d", len(list))
	}
	if list[0]["runtime_id"] != "rt_1" || list[0]["status"] != "running" {
		t.Errorf("result = %v, want [{runtime_id: rt_1, status: running}]", list)
	}
}

func TestServerClose(t *testing.T) {
	path, cleanup := startFakeSupervisor(t)
	defer cleanup()

	s, err := NewServer(Options{
		Transport:        "stdio",
		SupervisorSocket: path,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run1"})
	if err != nil {
		t.Fatalf("handleAvenorStatus before close: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}

	_, _, err = s.handleAvenorStatus(context.Background(), nil, statusArgs{RunID: "run1"})
	if err == nil {
		t.Error("expected error after close")
	}
	if !strings.Contains(err.Error(), "control client not available") {
		t.Errorf("expected 'control client not available' error, got: %v", err)
	}
}

package mcpserver

import (
	"context"
	"fmt"
	"strings"
	"testing"
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

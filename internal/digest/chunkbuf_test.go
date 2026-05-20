package digest

import (
	"strings"
	"testing"
)

func TestChunkBuffer(t *testing.T) {
	tests := []struct {
		name     string
		appends  []string
		wantDir  string
		wantLbl  string
		wantOK   bool
	}{
		{
			name:    "marker in single chunk",
			appends: []string{"[loop: exit]"},
			wantDir: "exit",
			wantOK:  true,
		},
		{
			name:    "marker split across chunks",
			appends: []string{"[lo", "op: exit]"},
			wantDir: "exit",
			wantOK:  true,
		},
		{
			name:    "marker on its own line",
			appends: []string{"some text\n", "[loop: exit]\n"},
			wantDir: "exit",
			wantOK:  true,
		},
		{
			name:    "inline loop marker ignored",
			appends: []string{"prose [loop: exit]"},
			wantOK:  false,
		},
		{
			name:    "buffer trim",
			appends: []string{strings.Repeat("a", 600) + "\n", "[loop: exit]\n"},
			wantDir: "exit",
			wantOK:  true,
		},
		{
			name:    "ScanLoopMarker after buffer exhausted",
			appends: []string{strings.Repeat("a", 1000) + "\n", "[loop: exit]\n"},
			wantDir: "exit",
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cb := NewChunkBuffer()
			for _, a := range tt.appends {
				cb.Append(a)
			}
			dir, label, ok := cb.ScanLoopMarker()
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v (dir=%q label=%q)", ok, tt.wantOK, dir, label)
			}
			if !ok {
				return
			}
			if dir != tt.wantDir {
				t.Errorf("directive = %q, want %q", dir, tt.wantDir)
			}
			if label != tt.wantLbl {
				t.Errorf("label = %q, want %q", label, tt.wantLbl)
			}
		})
	}
}

func TestChunkBufferStatusDeduplication(t *testing.T) {
	t.Run("status deduplication", func(t *testing.T) {
		cb := NewChunkBuffer()
		cb.Append("[status: working | test]")
		phase, label, ok := cb.ScanStatusMarker()
		if !ok || phase != "working" || label != "test" {
			t.Fatalf("first call: ok=%v phase=%q label=%q", ok, phase, label)
		}
		phase, label, ok = cb.ScanStatusMarker()
		if ok {
			t.Fatalf("second call: ok=%v, want false (duplicate)", ok)
		}
	})

	t.Run("different status is re-emitted", func(t *testing.T) {
		cb := NewChunkBufferLen(40)
		cb.Append("[status: working | first]")
		phase, label, ok := cb.ScanStatusMarker()
		if !ok || phase != "working" || label != "first" {
			t.Fatalf("first call: ok=%v phase=%q label=%q", ok, phase, label)
		}

		// Push the first marker out of the 40-char window, add a different status
		cb.Append(strings.Repeat("x", 30) + "[status: thinking | second]")
		phase, label, ok = cb.ScanStatusMarker()
		if !ok || phase != "thinking" || label != "second" {
			t.Fatalf("second call: ok=%v phase=%q label=%q", ok, phase, label)
		}
	})
}

func TestChunkBufferEmptyAppend(t *testing.T) {
	cb := NewChunkBuffer()
	cb.Append("hello")
	cb.Append("")
	if cb.Text() != "hello" {
		t.Fatalf("Text() = %q, want %q", cb.Text(), "hello")
	}
}

func TestChunkBufferLoopSeverityAccumulation(t *testing.T) {
	cb := NewChunkBuffer()
	cb.Append("[loop: abort | critical]\n")
	dir, label, ok := cb.ScanLoopMarker()
	if !ok || dir != "abort" || label != "critical" {
		t.Fatalf("first scan: ok=%v dir=%q label=%q", ok, dir, label)
	}

	cb.Append("[loop: exit | minor]\n")
	dir, label, ok = cb.ScanLoopMarker()
	if !ok || dir != "abort" || label != "critical" {
		t.Fatalf("second scan: dir=%q label=%q (abort should still win)", dir, label)
	}
}

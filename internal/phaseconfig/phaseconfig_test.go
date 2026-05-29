package phaseconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidatePhaseNames(t *testing.T) {
	t.Run("empty slices OK", func(t *testing.T) {
		if err := ValidatePhaseNames(); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("unique names OK", func(t *testing.T) {
		err := ValidatePhaseNames(
			[]Phase{{Name: "a"}},
			[]Phase{{Name: "b"}},
		)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("duplicate within same slice", func(t *testing.T) {
		err := ValidatePhaseNames([]Phase{{Name: "a"}, {Name: "a"}})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("duplicate across slices", func(t *testing.T) {
		err := ValidatePhaseNames([]Phase{{Name: "a"}}, []Phase{{Name: "a"}})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("(initial) reserved", func(t *testing.T) {
		err := ValidatePhaseNames([]Phase{{Name: "(initial)"}})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("empty name skipped", func(t *testing.T) {
		err := ValidatePhaseNames([]Phase{{Name: ""}}, []Phase{{Name: ""}})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})
}

func TestValidatePhaseHasPrompt(t *testing.T) {
	t.Run("prompt OK", func(t *testing.T) {
		if err := ValidatePhaseHasPrompt(Phase{Prompt: "hello"}); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("prompt_file OK", func(t *testing.T) {
		if err := ValidatePhaseHasPrompt(Phase{PromptFile: "p.txt"}); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("loop_file OK", func(t *testing.T) {
		if err := ValidatePhaseHasPrompt(Phase{LoopFile: "loop.json"}); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("team_file OK", func(t *testing.T) {
		if err := ValidatePhaseHasPrompt(Phase{TeamFile: "team.json"}); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("empty returns error", func(t *testing.T) {
		err := ValidatePhaseHasPrompt(Phase{})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt and loop_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseHasPrompt(Phase{Prompt: "p", LoopFile: "l.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt and team_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseHasPrompt(Phase{Prompt: "p", TeamFile: "t.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt_file and loop_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseHasPrompt(Phase{PromptFile: "p.txt", LoopFile: "l.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("loop_file and team_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseHasPrompt(Phase{LoopFile: "l.json", TeamFile: "t.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestValidatePhaseMutualExclusions(t *testing.T) {
	t.Run("no exclusions OK", func(t *testing.T) {
		if err := ValidatePhaseMutualExclusions(Phase{Name: "test"}); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("prompt and prompt_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseMutualExclusions(Phase{Name: "test", Prompt: "p", PromptFile: "f.txt"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt and loop_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseMutualExclusions(Phase{Name: "test", Prompt: "p", LoopFile: "l.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt_file and team_file mutually exclusive", func(t *testing.T) {
		err := ValidatePhaseMutualExclusions(Phase{Name: "test", PromptFile: "p.txt", TeamFile: "t.json"})
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestResolvePhaseFiles(t *testing.T) {
	t.Run("empty phases OK", func(t *testing.T) {
		err := ResolvePhaseFiles([]Phase{}, t.TempDir())
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
	})

	t.Run("prompt_file resolved", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "p.txt"), []byte("file content"), 0644); err != nil {
			t.Fatal(err)
		}
		phases := []Phase{{Name: "test", PromptFile: "p.txt"}}
		err := ResolvePhaseFiles(phases, dir)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if phases[0].Prompt != "file content" {
			t.Fatalf("Prompt = %q, want %q", phases[0].Prompt, "file content")
		}
		if phases[0].PromptFile != "" {
			t.Fatalf("PromptFile = %q, want empty", phases[0].PromptFile)
		}
	})

	t.Run("absolute prompt_file resolved", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "p.txt")
		if err := os.WriteFile(path, []byte("abs content"), 0644); err != nil {
			t.Fatal(err)
		}
		phases := []Phase{{Name: "test", PromptFile: path}}
		err := ResolvePhaseFiles(phases, dir)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if phases[0].Prompt != "abs content" {
			t.Fatalf("Prompt = %q, want %q", phases[0].Prompt, "abs content")
		}
	})

	t.Run("prompt_file not found returns error", func(t *testing.T) {
		phases := []Phase{{Name: "test", PromptFile: "nonexistent.txt"}}
		err := ResolvePhaseFiles(phases, t.TempDir())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("prompt and prompt_file mutually exclusive", func(t *testing.T) {
		phases := []Phase{{Name: "test", Prompt: "inline", PromptFile: "p.txt"}}
		err := ResolvePhaseFiles(phases, t.TempDir())
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestInsertInitialPrompt(t *testing.T) {
	t.Run("insert into empty", func(t *testing.T) {
		var pre []Phase
		InsertInitialPrompt(&pre, "hello")
		if len(pre) != 1 {
			t.Fatalf("len = %d, want 1", len(pre))
		}
		if pre[0].Name != "(initial)" {
			t.Fatalf("Name = %q, want %q", pre[0].Name, "(initial)")
		}
		if pre[0].Prompt != "hello" {
			t.Fatalf("Prompt = %q, want %q", pre[0].Prompt, "hello")
		}
	})

	t.Run("insert at front", func(t *testing.T) {
		pre := []Phase{{Name: "existing", Prompt: "already"}}
		InsertInitialPrompt(&pre, "first")
		if len(pre) != 2 {
			t.Fatalf("len = %d, want 2", len(pre))
		}
		if pre[0].Name != "(initial)" {
			t.Fatalf("Name = %q, want %q", pre[0].Name, "(initial)")
		}
		if pre[0].Prompt != "first" {
			t.Fatalf("Prompt = %q, want %q", pre[0].Prompt, "first")
		}
		if pre[1].Name != "existing" {
			t.Fatalf("Name = %q, want %q", pre[1].Name, "existing")
		}
	})
}

func TestLoopDirectiveSeverity(t *testing.T) {
	tests := []struct {
		directive string
		want      int
	}{
		{"abort", 3},
		{"exit", 2},
		{"continue", 1},
		{"unknown", 0},
		{"", 0},
	}
	for _, tt := range tests {
		if got := LoopDirectiveSeverity(tt.directive); got != tt.want {
			t.Errorf("LoopDirectiveSeverity(%q) = %d, want %d", tt.directive, got, tt.want)
		}
	}
}

func TestBackoffDuration(t *testing.T) {
	tests := []struct {
		retry int
		want  string
	}{
		{0, "2s"},
		{1, "4s"},
		{2, "8s"},
		{3, "16s"},
		{4, "30s"},
		{10, "30s"},
	}
	for _, tt := range tests {
		got := BackoffDuration(tt.retry)
		if got.String() != tt.want {
			t.Errorf("BackoffDuration(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}
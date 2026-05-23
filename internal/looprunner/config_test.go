package looprunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadLoopConfig(t *testing.T) {
	t.Parallel()

	t.Run("empty config returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"max_iterations":5}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "loop config: at least one of pre or loop must be non-empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("valid minimal config defaults max_iterations", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"build it"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxIterations != 10 {
			t.Fatalf("expected MaxIterations=10, got %d", cfg.MaxIterations)
		}
		if len(cfg.Pre) != 1 {
			t.Fatalf("expected 1 pre phase, got %d", len(cfg.Pre))
		}
		if cfg.Pre[0].Name != "build" || cfg.Pre[0].Prompt != "build it" {
			t.Fatalf("unexpected pre phase: %+v", cfg.Pre[0])
		}
	})

	t.Run("valid config with loop phases", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"max_iterations":3,"loop":[{"name":"test","prompt":"run tests"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxIterations != 3 {
			t.Fatalf("expected MaxIterations=3, got %d", cfg.MaxIterations)
		}
		if len(cfg.Loop) != 1 {
			t.Fatalf("expected 1 loop phase, got %d", len(cfg.Loop))
		}
		if cfg.Loop[0].Name != "test" || cfg.Loop[0].Prompt != "run tests" {
			t.Fatalf("unexpected loop phase: %+v", cfg.Loop[0])
		}
	})

	t.Run("loop with defaulted max_iterations", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"loop":[{"name":"test","prompt":"run tests"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxIterations != 10 {
			t.Fatalf("expected MaxIterations=10, got %d", cfg.MaxIterations)
		}
	})

	t.Run("phase missing name returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"prompt":"build it"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "loop config: phase[index 0]: name must not be empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("phase missing prompt returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"foo"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "loop config: phase[name foo]: prompt must not be empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("duplicate phase names across pre and loop", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"build it"}],"loop":[{"name":"build","prompt":"build again"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `loop config: duplicate phase name: "build"` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("explicit initial name in config is rejected", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"(initial)","prompt":"build it"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `loop config: duplicate phase name: "(initial)"` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("InsertInitialPrompt adds phase at index 0 of Pre", func(t *testing.T) {
		cfg := &LoopConfig{
			Pre: []Phase{{Name: "build", Prompt: "build it"}},
		}
		cfg.InsertInitialPrompt("my initial prompt")
		if len(cfg.Pre) != 2 {
			t.Fatalf("expected 2 pre phases, got %d", len(cfg.Pre))
		}
		if cfg.Pre[0].Name != "(initial)" {
			t.Fatalf("expected first phase name to be (initial), got %q", cfg.Pre[0].Name)
		}
		if cfg.Pre[0].Prompt != "my initial prompt" {
			t.Fatalf("expected first phase prompt to be %q, got %q", "my initial prompt", cfg.Pre[0].Prompt)
		}
		if cfg.Pre[1].Name != "build" {
			t.Fatalf("expected second phase name to be build, got %q", cfg.Pre[1].Name)
		}
	})

	t.Run("InsertInitialPrompt does not trigger duplicate error", func(t *testing.T) {
		cfg := &LoopConfig{
			Pre: []Phase{{Name: "foo", Prompt: "do foo"}},
		}
		cfg.InsertInitialPrompt("start here")
		if len(cfg.Pre) != 2 {
			t.Fatalf("expected 2 pre phases, got %d", len(cfg.Pre))
		}
		if cfg.Pre[0].Name != "(initial)" || cfg.Pre[1].Name != "foo" {
			t.Fatalf("unexpected phases: %+v", cfg.Pre)
		}
	})

	t.Run("malformed JSON returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{invalid}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("valid config with pre and loop", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"max_iterations":5,"pre":[{"name":"setup","prompt":"setup env"}],"loop":[{"name":"test","prompt":"run tests"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxIterations != 5 {
			t.Fatalf("expected MaxIterations=5, got %d", cfg.MaxIterations)
		}
		if len(cfg.Pre) != 1 || len(cfg.Loop) != 1 {
			t.Fatalf("expected 1 pre and 1 loop, got %d pre, %d loop", len(cfg.Pre), len(cfg.Loop))
		}
	})

	t.Run("loop phase missing name returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"loop":[{"prompt":"run tests"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "loop config: phase[index 0]: name must not be empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("loop phase missing prompt returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"loop":[{"name":"test"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "loop config: phase[name test]: prompt must not be empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("max_iterations explicit zero defaults to 10", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"max_iterations":0,"loop":[{"name":"test","prompt":"run tests"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MaxIterations != 10 {
			t.Fatalf("expected MaxIterations=10 (defaulted), got %d", cfg.MaxIterations)
		}
	})
}

func TestValidatePhaseMissingNamePre(t *testing.T) {
	cfg := &LoopConfig{Pre: []Phase{{Prompt: "hello"}}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "loop config: phase[index 0]: name must not be empty" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidatePhaseMissingPromptLoop(t *testing.T) {
	cfg := &LoopConfig{Loop: []Phase{{Name: "test"}}, MaxIterations: 5}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "loop config: phase[name test]: prompt must not be empty" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateNameInPre(t *testing.T) {
	cfg := &LoopConfig{Pre: []Phase{
		{Name: "foo", Prompt: "first"},
		{Name: "foo", Prompt: "second"},
	}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `loop config: duplicate phase name: "foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateNameInLoop(t *testing.T) {
	cfg := &LoopConfig{
		Pre:           []Phase{{Name: "foo", Prompt: "first"}},
		Loop:          []Phase{{Name: "foo", Prompt: "second"}},
		MaxIterations: 5,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `loop config: duplicate phase name: "foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateAcrossMultipleLoop(t *testing.T) {
	cfg := &LoopConfig{
		Loop: []Phase{
			{Name: "a", Prompt: "a"},
			{Name: "b", Prompt: "b"},
			{Name: "a", Prompt: "a again"},
		},
		MaxIterations: 5,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `loop config: duplicate phase name: "a"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateOK(t *testing.T) {
	cfg := &LoopConfig{
		MaxIterations: 10,
		Pre:           []Phase{{Name: "setup", Prompt: "setup env"}},
		Loop:          []Phase{{Name: "test", Prompt: "run tests"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertInitialPromptOnEmptyPre(t *testing.T) {
	cfg := &LoopConfig{Pre: nil}
	cfg.InsertInitialPrompt("hello")
	if len(cfg.Pre) != 1 {
		t.Fatalf("expected 1 pre phase, got %d", len(cfg.Pre))
	}
	if cfg.Pre[0].Name != "(initial)" || cfg.Pre[0].Prompt != "hello" {
		t.Fatalf("unexpected phase: %+v", cfg.Pre[0])
	}
}

func TestInsertInitialPromptResumeFromPrevious(t *testing.T) {
	cfg := &LoopConfig{Pre: []Phase{{Name: "build", Prompt: "build it", ResumeFromPrevious: true}}}
	cfg.InsertInitialPrompt("go")
	if len(cfg.Pre) != 2 {
		t.Fatalf("expected 2 pre phases, got %d", len(cfg.Pre))
	}
	if cfg.Pre[0].Name != "(initial)" || cfg.Pre[0].Prompt != "go" {
		t.Fatalf("unexpected initial phase: %+v", cfg.Pre[0])
	}
	if cfg.Pre[1].Name != "build" || cfg.Pre[1].Prompt != "build it" || !cfg.Pre[1].ResumeFromPrevious {
		t.Fatalf("unexpected second phase: %+v", cfg.Pre[1])
	}
}

func TestValidateNamesCaseSensitiveDistinct(t *testing.T) {
	cfg := &LoopConfig{
		Pre: []Phase{
			{Name: "Foo", Prompt: "first"},
			{Name: "foo", Prompt: "second"},
		},
	}
	err := cfg.Validate()
	if err != nil {
		t.Fatalf("expected no error for case-sensitive distinct names, got: %v", err)
	}
}

func TestValidateNamesCaseSensitiveDuplicate(t *testing.T) {
	cfg := &LoopConfig{
		Pre: []Phase{
			{Name: "Foo", Prompt: "first"},
			{Name: "Foo", Prompt: "second"},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate with same case, got nil")
	}
	if err.Error() != `loop config: duplicate phase name: "Foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadLoopConfigFileNotFound(t *testing.T) {
	cfg, err := LoadLoopConfig("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if cfg != nil {
		t.Fatal("expected nil config on error")
	}
}

func TestResumeFromPreviousRoundTrip(t *testing.T) {
	cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"build it","resume_from_previous":true}]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Pre[0].ResumeFromPrevious {
		t.Fatal("expected ResumeFromPrevious to be true")
	}
}

func TestPromptFile(t *testing.T) {
	t.Parallel()

	t.Run("relative prompt_file resolved from config dir", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "build.txt"), []byte("build the project"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"pre":[{"name":"build","prompt_file":"build.txt"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pre[0].Prompt != "build the project" {
			t.Fatalf("unexpected prompt: %q", cfg.Pre[0].Prompt)
		}
		if cfg.Pre[0].PromptFile != "" {
			t.Fatalf("expected PromptFile to be cleared, got %q", cfg.Pre[0].PromptFile)
		}
	})

	t.Run("absolute prompt_file path works", func(t *testing.T) {
		dir := t.TempDir()
		absPath := filepath.Join(dir, "prompt.txt")
		if err := os.WriteFile(absPath, []byte("absolute prompt"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"pre":[{"name":"p","prompt_file":"`+absPath+`"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pre[0].Prompt != "absolute prompt" {
			t.Fatalf("unexpected prompt: %q", cfg.Pre[0].Prompt)
		}
	})

	t.Run("prompt and prompt_file both set returns error", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "p.txt"), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"pre":[{"name":"build","prompt":"inline","prompt_file":"p.txt"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `loop config: phase[name build]: prompt and prompt_file are mutually exclusive` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt_file not found returns error", func(t *testing.T) {
		dir := t.TempDir()
		cfg, err := loadFromDir(t, dir, `{"pre":[{"name":"build","prompt_file":"missing.txt"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "reading prompt_file") || !strings.Contains(err.Error(), "missing.txt") {
			t.Fatalf("expected error to mention \"reading prompt_file\" and filename, got: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt_file in loop phase works", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("run tests"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"loop":[{"name":"test","prompt_file":"test.txt"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Loop[0].Prompt != "run tests" {
			t.Fatalf("unexpected prompt: %q", cfg.Loop[0].Prompt)
		}
		if cfg.Loop[0].PromptFile != "" {
			t.Fatalf("expected PromptFile to be cleared, got %q", cfg.Loop[0].PromptFile)
		}
	})

	t.Run("prompt_file content preserves whitespace", func(t *testing.T) {
		dir := t.TempDir()
		content := "line one\n\nline two\n"
		if err := os.WriteFile(filepath.Join(dir, "p.txt"), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"pre":[{"name":"p","prompt_file":"p.txt"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Pre[0].Prompt != content {
			t.Fatalf("unexpected prompt: %q", cfg.Pre[0].Prompt)
		}
	})
}

// helpers

func loadFromJSON(t *testing.T, data string) (*LoopConfig, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		return nil, err
	}
	return LoadLoopConfig(path)
}

func loadFromDir(t *testing.T, dir, data string) (*LoopConfig, error) {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		return nil, err
	}
	return LoadLoopConfig(path)
}

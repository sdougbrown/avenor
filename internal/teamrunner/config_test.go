package teamrunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

func TestLoadTeamConfig(t *testing.T) {
	t.Parallel()

	t.Run("empty config returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "team config: at least one of pre or team must be non-empty" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("valid config with only pre", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"setup","prompt":"setup env"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Pre) != 1 {
			t.Fatalf("expected 1 pre phase, got %d", len(cfg.Pre))
		}
		if cfg.Pre[0].Name != "setup" || cfg.Pre[0].Prompt != "setup env" {
			t.Fatalf("unexpected pre phase: %+v", cfg.Pre[0])
		}
	})

	t.Run("valid config with only team", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","prompt":"do work"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Team) != 1 {
			t.Fatalf("expected 1 team phase, got %d", len(cfg.Team))
		}
		if cfg.Team[0].Name != "work" || cfg.Team[0].Prompt != "do work" {
			t.Fatalf("unexpected team phase: %+v", cfg.Team[0])
		}
	})

	t.Run("valid config with pre, team, and post", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"setup","prompt":"setup"}],"team":[{"name":"work","prompt":"work"}],"post":[{"name":"report","prompt":"report"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Pre) != 1 || len(cfg.Team) != 1 || len(cfg.Post) != 1 {
			t.Fatalf("expected 1 phase each, got pre=%d team=%d post=%d", len(cfg.Pre), len(cfg.Team), len(cfg.Post))
		}
	})

	t.Run("phase missing name returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"prompt":"build it"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != "team config: phase[index 0]: name must not be empty" {
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
		if err.Error() != "team config: phase[name foo]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)" {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("duplicate phase names across sections returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"build it"}],"team":[{"name":"build","prompt":"build again"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: duplicate phase name: "build"` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("(initial) reserved name returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"(initial)","prompt":"build it"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: duplicate phase name: "(initial)"` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt and prompt_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"inline","prompt_file":"build.txt"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: prompt and prompt_file are mutually exclusive` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt and loop_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"inline","loop_file":"loop.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: prompt is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt and team_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"pre":[{"name":"build","prompt":"inline","team_file":"team.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: prompt is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("loop_file and team_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"build","loop_file":"loop.json","team_file":"team.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: loop_file and team_file are mutually exclusive` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt_file and loop_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"build","prompt_file":"p.txt","loop_file":"loop.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: prompt_file is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt_file and team_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"build","prompt_file":"p.txt","team_file":"team.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name build]: prompt_file is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
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
}

func TestValidatePhaseMissingNameTeam(t *testing.T) {
	cfg := &TeamConfig{Team: []phaseconfig.Phase{{Prompt: "hello"}}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "team config: phase[index 0]: name must not be empty" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidatePhaseMissingPromptTeam(t *testing.T) {
	cfg := &TeamConfig{Team: []phaseconfig.Phase{{Name: "test"}}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "team config: phase[name test]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateNameInPre(t *testing.T) {
	cfg := &TeamConfig{Pre: []phaseconfig.Phase{
		{Name: "foo", Prompt: "first"},
		{Name: "foo", Prompt: "second"},
	}}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateNameInTeam(t *testing.T) {
	cfg := &TeamConfig{
		Pre:  []phaseconfig.Phase{{Name: "foo", Prompt: "first"}},
		Team: []phaseconfig.Phase{{Name: "foo", Prompt: "second"}},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateDuplicateAcrossMultipleTeam(t *testing.T) {
	cfg := &TeamConfig{
		Team: []phaseconfig.Phase{
			{Name: "a", Prompt: "a"},
			{Name: "b", Prompt: "b"},
			{Name: "a", Prompt: "a again"},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "a"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateOK(t *testing.T) {
	cfg := &TeamConfig{
		Pre:  []phaseconfig.Phase{{Name: "setup", Prompt: "setup env"}},
		Team: []phaseconfig.Phase{{Name: "work", Prompt: "do work"}},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInsertInitialPromptOnEmptyPre(t *testing.T) {
	cfg := &TeamConfig{Pre: nil}
	cfg.InsertInitialPrompt("hello")
	if len(cfg.Pre) != 1 {
		t.Fatalf("expected 1 pre phase, got %d", len(cfg.Pre))
	}
	if cfg.Pre[0].Name != "(initial)" || cfg.Pre[0].Prompt != "hello" {
		t.Fatalf("unexpected phase: %+v", cfg.Pre[0])
	}
}

func TestInsertInitialPromptOnExistingPre(t *testing.T) {
	cfg := &TeamConfig{
		Pre:  []phaseconfig.Phase{{Name: "build", Prompt: "build it"}},
		Team: []phaseconfig.Phase{{Name: "work", Prompt: "do work"}},
	}
	cfg.InsertInitialPrompt("start here")
	if len(cfg.Pre) != 2 {
		t.Fatalf("expected 2 pre phases, got %d", len(cfg.Pre))
	}
	if cfg.Pre[0].Name != "(initial)" || cfg.Pre[0].Prompt != "start here" {
		t.Fatalf("unexpected initial phase: %+v", cfg.Pre[0])
	}
	if cfg.Pre[1].Name != "build" || cfg.Pre[1].Prompt != "build it" {
		t.Fatalf("unexpected second phase: %+v", cfg.Pre[1])
	}
}

func TestInsertInitialPromptResumeFromPrevious(t *testing.T) {
	cfg := &TeamConfig{Pre: []phaseconfig.Phase{{Name: "build", Prompt: "build it", ResumeFromPrevious: true}}}
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
	cfg := &TeamConfig{
		Pre: []phaseconfig.Phase{
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
	cfg := &TeamConfig{
		Pre: []phaseconfig.Phase{
			{Name: "Foo", Prompt: "first"},
			{Name: "Foo", Prompt: "second"},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate with same case, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "Foo"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadTeamConfigFileNotFound(t *testing.T) {
	cfg, err := LoadTeamConfig("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if cfg != nil {
		t.Fatal("expected nil config on error")
	}
}

func TestLoadTeamConfigWithPost(t *testing.T) {
	cfg, err := loadFromJSON(t, `{
		"team": [{"name":"work","prompt":"work"}],
		"post": [{"name":"report","prompt":"report"}]
	}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Post) != 1 {
		t.Fatalf("expected 1 post phase, got %d", len(cfg.Post))
	}
	if cfg.Post[0].Name != "report" || cfg.Post[0].Prompt != "report" {
		t.Fatalf("unexpected post phase: %+v", cfg.Post[0])
	}
}

func TestValidateDuplicateNameAcrossPost(t *testing.T) {
	cfg := &TeamConfig{
		Team: []phaseconfig.Phase{{Name: "work", Prompt: "work"}},
		Post: []phaseconfig.Phase{{Name: "work", Prompt: "also work"}},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for duplicate name across team and post, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "work"` {
		t.Fatalf("unexpected error: %v", err)
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

func TestConditionalAgentModelFields(t *testing.T) {
	cfg, err := loadFromJSON(t, `{"pre":[{"name":"decide","prompt":"decide"}],"team":[{"name":"analyze","prompt":"analyze it","conditional":true,"agent":"coder","model":"claude"}]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Team[0].Conditional {
		t.Fatal("expected Conditional to be true")
	}
	if cfg.Team[0].Agent != "coder" {
		t.Fatalf("expected Agent=coder, got %q", cfg.Team[0].Agent)
	}
	if cfg.Team[0].Model != "claude" {
		t.Fatalf("expected Model=claude, got %q", cfg.Team[0].Model)
	}
}

func TestConditionalMembersRequirePrePhase(t *testing.T) {
	cfg, err := loadFromJSON(t, `{"team":[{"name":"analyze","prompt":"analyze it","conditional":true}]}`)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "team config: conditional team members require at least one pre phase" {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil config on error")
	}
}

func TestValidConfigWithTeamFile(t *testing.T) {
	cfg, err := loadFromJSON(t, `{"team":[{"name":"work","team_file":"team.json"}]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Team[0].TeamFile != "team.json" {
		t.Fatalf("expected TeamFile=team.json, got %q", cfg.Team[0].TeamFile)
	}
	if cfg.Team[0].Prompt != "" {
		t.Fatalf("expected Prompt to be empty, got %q", cfg.Team[0].Prompt)
	}
}

func TestValidConfigWithLoopFile(t *testing.T) {
	cfg, err := loadFromJSON(t, `{"team":[{"name":"work","loop_file":"loop.json"}]}`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Team[0].LoopFile != "loop.json" {
		t.Fatalf("expected LoopFile=loop.json, got %q", cfg.Team[0].LoopFile)
	}
	if cfg.Team[0].Prompt != "" {
		t.Fatalf("expected Prompt to be empty, got %q", cfg.Team[0].Prompt)
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
		if err.Error() != `team config: phase[name build]: prompt and prompt_file are mutually exclusive` {
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

	t.Run("prompt_file in team phase works", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("run tests"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"team":[{"name":"test","prompt_file":"test.txt"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Team[0].Prompt != "run tests" {
			t.Fatalf("unexpected prompt: %q", cfg.Team[0].Prompt)
		}
		if cfg.Team[0].PromptFile != "" {
			t.Fatalf("expected PromptFile to be cleared, got %q", cfg.Team[0].PromptFile)
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

func TestDuplicateNameInPost(t *testing.T) {
	cfg := &TeamConfig{
		Pre: []phaseconfig.Phase{{Name: "setup", Prompt: "setup"}},
		Post: []phaseconfig.Phase{
			{Name: "a", Prompt: "a"},
			{Name: "a", Prompt: "a again"},
		},
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != `team config: duplicate phase name: "a"` {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoopFileAndTeamFileMutualExclusion(t *testing.T) {
	t.Parallel()

	t.Run("prompt and loop_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","prompt":"do work","loop_file":"sub.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name work]: prompt is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("prompt and team_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","prompt":"do work","team_file":"team.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name work]: prompt is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("loop_file and team_file both set returns error", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","loop_file":"loop.json","team_file":"team.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name work]: loop_file and team_file are mutually exclusive` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})

	t.Run("only loop_file set is valid", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","loop_file":"sub.json"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Team[0].LoopFile != "sub.json" {
			t.Fatalf("expected LoopFile to be sub.json, got %q", cfg.Team[0].LoopFile)
		}
	})

	t.Run("only team_file set is valid", func(t *testing.T) {
		cfg, err := loadFromJSON(t, `{"team":[{"name":"work","team_file":"team.json"}]}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Team[0].TeamFile != "team.json" {
			t.Fatalf("expected TeamFile to be team.json, got %q", cfg.Team[0].TeamFile)
		}
	})

	t.Run("prompt_file and loop_file both set returns error", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "p.txt"), []byte("content"), 0644); err != nil {
			t.Fatal(err)
		}
		cfg, err := loadFromDir(t, dir, `{"team":[{"name":"work","prompt_file":"p.txt","loop_file":"sub.json"}]}`)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if err.Error() != `team config: phase[name work]: prompt_file is mutually exclusive with loop_file and team_file` {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg != nil {
			t.Fatal("expected nil config on error")
		}
	})
}

// helpers

func loadFromJSON(t *testing.T, data string) (*TeamConfig, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		return nil, err
	}
	return LoadTeamConfig(path)
}

func loadFromDir(t *testing.T, dir, data string) (*TeamConfig, error) {
	t.Helper()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		return nil, err
	}
	return LoadTeamConfig(path)
}

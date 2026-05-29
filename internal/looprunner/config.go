package looprunner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Phase struct {
	Name               string `json:"name"`
	Prompt             string `json:"prompt"`
	PromptFile         string `json:"prompt_file,omitempty"`
	LoopFile           string `json:"loop_file,omitempty"`
	TeamFile           string `json:"team_file,omitempty"`
	ResumeFromPrevious bool   `json:"resume_from_previous,omitempty"`
}

type LoopConfig struct {
	MaxIterations int     `json:"max_iterations"`
	Pre           []Phase `json:"pre"`
	Loop          []Phase `json:"loop"`
	Post          []Phase `json:"post"`
}

func LoadLoopConfig(path string) (*LoopConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg LoopConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.MaxIterations == 0 {
		cfg.MaxIterations = 10
	}

	configDir := filepath.Dir(path)
	if err := cfg.preValidate(); err != nil {
		return nil, err
	}
	if err := resolvePromptFiles(cfg.Pre, configDir); err != nil {
		return nil, err
	}
	if err := resolvePromptFiles(cfg.Loop, configDir); err != nil {
		return nil, err
	}
	if err := resolvePromptFiles(cfg.Post, configDir); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *LoopConfig) preValidate() error {
	for i := range c.Pre {
		if err := validateMutualExclusions(&c.Pre[i]); err != nil {
			return err
		}
	}
	for i := range c.Loop {
		if err := validateMutualExclusions(&c.Loop[i]); err != nil {
			return err
		}
	}
	for i := range c.Post {
		if err := validateMutualExclusions(&c.Post[i]); err != nil {
			return err
		}
	}
	return nil
}

func validateMutualExclusions(p *Phase) error {
	if p.Prompt != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("loop config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.PromptFile != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("loop config: phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.LoopFile != "" && p.TeamFile != "" {
		return fmt.Errorf("loop config: phase[name %s]: loop_file and team_file are mutually exclusive", p.Name)
	}
	return nil
}

func resolvePromptFiles(phases []Phase, configDir string) error {
	for i := range phases {
		p := &phases[i]
		if p.PromptFile == "" {
			continue
		}
		if p.Prompt != "" {
			return fmt.Errorf("loop config: phase[name %s]: prompt and prompt_file are mutually exclusive", p.Name)
		}
		absPath := p.PromptFile
		if !filepath.IsAbs(absPath) {
			absPath = filepath.Join(configDir, absPath)
		}
		contents, err := os.ReadFile(absPath)
		if err != nil {
			return fmt.Errorf("loop config: phase[name %s]: reading prompt_file %q: %w", p.Name, p.PromptFile, err)
		}
		p.Prompt = string(contents)
		p.PromptFile = ""
	}
	return nil
}

func (c *LoopConfig) Validate() error {
	if len(c.Pre) == 0 && len(c.Loop) == 0 {
		return fmt.Errorf("loop config: at least one of pre or loop must be non-empty")
	}

	if len(c.Loop) > 0 && c.MaxIterations < 1 {
		return fmt.Errorf("loop config: max_iterations must be >= 1 when loop phases are present")
	}

	for i := range c.Pre {
		if c.Pre[i].Name == "" {
			return fmt.Errorf("loop config: phase[index %d]: name must not be empty", i)
		}
		if c.Pre[i].Prompt == "" && c.Pre[i].LoopFile == "" && c.Pre[i].TeamFile == "" {
			return fmt.Errorf("loop config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Pre[i].Name)
		}
		if c.Pre[i].Prompt != "" && (c.Pre[i].LoopFile != "" || c.Pre[i].TeamFile != "") {
			return fmt.Errorf("loop config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Pre[i].Name)
		}
		if c.Pre[i].LoopFile != "" && c.Pre[i].TeamFile != "" {
			return fmt.Errorf("loop config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Pre[i].Name)
		}
	}

	for i := range c.Loop {
		if c.Loop[i].Name == "" {
			return fmt.Errorf("loop config: phase[index %d]: name must not be empty", i)
		}
		if c.Loop[i].Prompt == "" && c.Loop[i].LoopFile == "" && c.Loop[i].TeamFile == "" {
			return fmt.Errorf("loop config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Loop[i].Name)
		}
		if c.Loop[i].Prompt != "" && (c.Loop[i].LoopFile != "" || c.Loop[i].TeamFile != "") {
			return fmt.Errorf("loop config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Loop[i].Name)
		}
		if c.Loop[i].LoopFile != "" && c.Loop[i].TeamFile != "" {
			return fmt.Errorf("loop config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Loop[i].Name)
		}
	}

	for i := range c.Post {
		if c.Post[i].Name == "" {
			return fmt.Errorf("loop config: phase[index %d]: name must not be empty", i)
		}
		if c.Post[i].Prompt == "" && c.Post[i].LoopFile == "" && c.Post[i].TeamFile == "" {
			return fmt.Errorf("loop config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Post[i].Name)
		}
		if c.Post[i].Prompt != "" && (c.Post[i].LoopFile != "" || c.Post[i].TeamFile != "") {
			return fmt.Errorf("loop config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Post[i].Name)
		}
		if c.Post[i].LoopFile != "" && c.Post[i].TeamFile != "" {
			return fmt.Errorf("loop config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Post[i].Name)
		}
	}

	names := make(map[string]struct{})
	for i := range c.Pre {
		n := c.Pre[i].Name
		if n == "(initial)" {
			return fmt.Errorf("loop config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("loop config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Loop {
		n := c.Loop[i].Name
		if n == "(initial)" {
			return fmt.Errorf("loop config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("loop config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Post {
		n := c.Post[i].Name
		if n == "(initial)" {
			return fmt.Errorf("loop config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("loop config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	return nil
}

func (c *LoopConfig) InsertInitialPrompt(prompt string) {
	c.Pre = append([]Phase{{Name: "(initial)", Prompt: prompt}}, c.Pre...)
}

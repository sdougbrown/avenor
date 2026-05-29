package teamrunner

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
	Conditional        bool   `json:"conditional,omitempty"`
	Agent              string `json:"agent,omitempty"`
	Model              string `json:"model,omitempty"`
}

type TeamConfig struct {
	Pre  []Phase `json:"pre"`
	Team []Phase `json:"team"`
	Post []Phase `json:"post"`
}

func LoadTeamConfig(path string) (*TeamConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg TeamConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if err := cfg.preValidate(); err != nil {
		return nil, err
	}

	configDir := filepath.Dir(path)
	if err := resolvePromptFiles(cfg.Pre, configDir); err != nil {
		return nil, err
	}
	if err := resolvePromptFiles(cfg.Team, configDir); err != nil {
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

func (c *TeamConfig) preValidate() error {
	for i := range c.Pre {
		if err := validateMutualExclusions(&c.Pre[i]); err != nil {
			return err
		}
	}
	for i := range c.Team {
		if err := validateMutualExclusions(&c.Team[i]); err != nil {
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
		return fmt.Errorf("team config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.PromptFile != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("team config: phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.LoopFile != "" && p.TeamFile != "" {
		return fmt.Errorf("team config: phase[name %s]: loop_file and team_file are mutually exclusive", p.Name)
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
			return fmt.Errorf("team config: phase[name %s]: prompt and prompt_file are mutually exclusive", p.Name)
		}
		absPath := p.PromptFile
		if !filepath.IsAbs(absPath) {
			absPath = filepath.Join(configDir, absPath)
		}
		contents, err := os.ReadFile(absPath)
		if err != nil {
			return fmt.Errorf("team config: phase[name %s]: reading prompt_file %q: %w", p.Name, p.PromptFile, err)
		}
		p.Prompt = string(contents)
		p.PromptFile = ""
	}
	return nil
}

func (c *TeamConfig) Validate() error {
	if len(c.Pre) == 0 && len(c.Team) == 0 {
		return fmt.Errorf("team config: at least one of pre or team must be non-empty")
	}

	names := make(map[string]struct{})

	for i := range c.Pre {
		if c.Pre[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if c.Pre[i].Prompt == "" && c.Pre[i].PromptFile == "" && c.Pre[i].LoopFile == "" && c.Pre[i].TeamFile == "" {
			return fmt.Errorf("team config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Pre[i].Name)
		}
		if c.Pre[i].Prompt != "" && (c.Pre[i].LoopFile != "" || c.Pre[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Pre[i].Name)
		}
		if c.Pre[i].PromptFile != "" && (c.Pre[i].LoopFile != "" || c.Pre[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", c.Pre[i].Name)
		}
		if c.Pre[i].LoopFile != "" && c.Pre[i].TeamFile != "" {
			return fmt.Errorf("team config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Pre[i].Name)
		}
		n := c.Pre[i].Name
		if n == "(initial)" {
			return fmt.Errorf("team config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("team config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Team {
		if c.Team[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if c.Team[i].Prompt == "" && c.Team[i].PromptFile == "" && c.Team[i].LoopFile == "" && c.Team[i].TeamFile == "" {
			return fmt.Errorf("team config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Team[i].Name)
		}
		if c.Team[i].Prompt != "" && (c.Team[i].LoopFile != "" || c.Team[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Team[i].Name)
		}
		if c.Team[i].PromptFile != "" && (c.Team[i].LoopFile != "" || c.Team[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", c.Team[i].Name)
		}
		if c.Team[i].LoopFile != "" && c.Team[i].TeamFile != "" {
			return fmt.Errorf("team config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Team[i].Name)
		}
		n := c.Team[i].Name
		if n == "(initial)" {
			return fmt.Errorf("team config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("team config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Post {
		if c.Post[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if c.Post[i].Prompt == "" && c.Post[i].PromptFile == "" && c.Post[i].LoopFile == "" && c.Post[i].TeamFile == "" {
			return fmt.Errorf("team config: phase[name %s]: prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)", c.Post[i].Name)
		}
		if c.Post[i].Prompt != "" && (c.Post[i].LoopFile != "" || c.Post[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt is mutually exclusive with loop_file and team_file", c.Post[i].Name)
		}
		if c.Post[i].PromptFile != "" && (c.Post[i].LoopFile != "" || c.Post[i].TeamFile != "") {
			return fmt.Errorf("team config: phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", c.Post[i].Name)
		}
		if c.Post[i].LoopFile != "" && c.Post[i].TeamFile != "" {
			return fmt.Errorf("team config: phase[name %s]: loop_file and team_file are mutually exclusive", c.Post[i].Name)
		}
		n := c.Post[i].Name
		if n == "(initial)" {
			return fmt.Errorf("team config: duplicate phase name: \"(initial)\"")
		}
		if _, ok := names[n]; ok {
			return fmt.Errorf("team config: duplicate phase name: \"%s\"", n)
		}
		names[n] = struct{}{}
	}

	return nil
}

func (c *TeamConfig) InsertInitialPrompt(prompt string) {
	c.Pre = append([]Phase{{Name: "(initial)", Prompt: prompt}}, c.Pre...)
}
package teamrunner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

type TeamConfig struct {
	Pre  []phaseconfig.Phase `json:"pre"`
	Team []phaseconfig.Phase `json:"team"`
	Post []phaseconfig.Phase `json:"post"`
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
	if err := phaseconfig.ResolvePhaseFiles(cfg.Pre, configDir); err != nil {
		return nil, err
	}
	if err := phaseconfig.ResolvePhaseFiles(cfg.Team, configDir); err != nil {
		return nil, err
	}
	if err := phaseconfig.ResolvePhaseFiles(cfg.Post, configDir); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *TeamConfig) preValidate() error {
	for i := range c.Pre {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Pre[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
	}
	for i := range c.Team {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Team[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
	}
	for i := range c.Post {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Post[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
	}
	return nil
}



func (c *TeamConfig) Validate() error {
	if len(c.Pre) == 0 && len(c.Team) == 0 {
		return fmt.Errorf("team config: at least one of pre or team must be non-empty")
	}
	if len(c.Pre) == 0 {
		for _, phase := range c.Team {
			if phase.Conditional {
				return fmt.Errorf("team config: conditional team members require at least one pre phase")
			}
		}
	}

	names := make(map[string]struct{})

	for i := range c.Pre {
		if c.Pre[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Pre[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Pre[i].Name, err)
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
			return fmt.Errorf("team config: duplicate phase name: %q", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Team {
		if c.Team[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Team[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Team[i].Name, err)
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
			return fmt.Errorf("team config: duplicate phase name: %q", n)
		}
		names[n] = struct{}{}
	}

	for i := range c.Post {
		if c.Post[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Post[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Post[i].Name, err)
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
			return fmt.Errorf("team config: duplicate phase name: %q", n)
		}
		names[n] = struct{}{}
	}

	return nil
}

func (c *TeamConfig) InsertInitialPrompt(prompt string) {
	phaseconfig.InsertInitialPrompt(&c.Pre, prompt)
}

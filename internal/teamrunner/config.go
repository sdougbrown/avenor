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

	for i := range c.Pre {
		if c.Pre[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Pre[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Pre[i].Name, err)
		}
	}

	for i := range c.Team {
		if c.Team[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Team[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Team[i].Name, err)
		}
	}

	for i := range c.Post {
		if c.Post[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Post[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Post[i].Name, err)
		}
	}

	if err := phaseconfig.ValidatePhaseNames(c.Pre, c.Team, c.Post); err != nil {
		return fmt.Errorf("team config: %w", err)
	}

	return nil
}

func (c *TeamConfig) InsertInitialPrompt(prompt string) {
	phaseconfig.InsertInitialPrompt(&c.Pre, prompt)
}

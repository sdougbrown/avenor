package teamrunner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
)

type TeamConfig struct {
	RosterFile string              `json:"roster_file,omitempty"`
	Pre        []phaseconfig.Phase `json:"pre"`
	Team       []phaseconfig.Phase `json:"team"`
	Post       []phaseconfig.Phase `json:"post"`
}

func LoadTeamConfig(path string) (*TeamConfig, error) {
	cfg, _, err := LoadTeamConfigWithRoster(path, nil, "")
	return cfg, err
}

// LoadTeamConfigWithRoster loads a team config and resolves its effective
// roster. A declared roster file is relative to the team config, then the
// inherited loaded roster is used, and finally fallbackPath is used.
func LoadTeamConfigWithRoster(path string, inherited *rosterconfig.Config, fallbackPath string) (*TeamConfig, *rosterconfig.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	var cfg TeamConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, nil, err
	}

	if err := cfg.preValidate(); err != nil {
		return nil, nil, err
	}

	configDir := filepath.Dir(path)
	if err := phaseconfig.ResolvePhaseFiles(cfg.Pre, configDir); err != nil {
		return nil, nil, err
	}
	if err := phaseconfig.ResolvePhaseFiles(cfg.Team, configDir); err != nil {
		return nil, nil, err
	}
	if err := phaseconfig.ResolvePhaseFiles(cfg.Post, configDir); err != nil {
		return nil, nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, nil, err
	}

	roster, err := rosterconfig.LoadForConfig(path, cfg.RosterFile, inherited, fallbackPath)
	if err != nil {
		return nil, nil, err
	}
	if err := validateRosterEntries(roster, cfg.Pre, cfg.Team, cfg.Post); err != nil {
		return nil, nil, err
	}

	return &cfg, roster, nil
}

func validateRosterEntries(roster *rosterconfig.Config, phases ...[]phaseconfig.Phase) error {
	for _, slice := range phases {
		for _, phase := range slice {
			if phase.RosterEntry == "" {
				continue
			}
			if roster == nil {
				return fmt.Errorf("team config: phase[name %s]: roster entry %q requires a roster file", phase.Name, phase.RosterEntry)
			}
			if _, err := roster.Lookup(phase.RosterEntry); err != nil {
				return fmt.Errorf("team config: phase[name %s]: %w", phase.Name, err)
			}
		}
	}
	return nil
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
		if err := phaseconfig.ValidatePhaseRoster(c.Pre[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
		if c.Pre[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Pre[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Pre[i].Name, err)
		}
	}

	for i := range c.Team {
		if err := phaseconfig.ValidatePhaseRoster(c.Team[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
		if c.Team[i].Name == "" {
			return fmt.Errorf("team config: phase[index %d]: name must not be empty", i)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Team[i]); err != nil {
			return fmt.Errorf("team config: phase[name %s]: %w", c.Team[i].Name, err)
		}
	}

	for i := range c.Post {
		if err := phaseconfig.ValidatePhaseRoster(c.Post[i]); err != nil {
			return fmt.Errorf("team config: %w", err)
		}
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

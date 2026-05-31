package looprunner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/phaseconfig"
)

type LoopConfig struct {
	MaxIterations int                 `json:"max_iterations"`
	Pre           []phaseconfig.Phase `json:"pre"`
	Loop          []phaseconfig.Phase `json:"loop"`
	Post          []phaseconfig.Phase `json:"post"`
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
	if err := phaseconfig.ResolvePhaseFiles(cfg.Pre, configDir); err != nil {
		return nil, err
	}
	if err := phaseconfig.ResolvePhaseFiles(cfg.Loop, configDir); err != nil {
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

func (c *LoopConfig) preValidate() error {
	for i := range c.Pre {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Pre[i]); err != nil {
			return fmt.Errorf("loop config: %w", err)
		}
	}
	for i := range c.Loop {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Loop[i]); err != nil {
			return fmt.Errorf("loop config: %w", err)
		}
	}
	for i := range c.Post {
		if err := phaseconfig.ValidatePhaseMutualExclusions(c.Post[i]); err != nil {
			return fmt.Errorf("loop config: %w", err)
		}
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
		if c.Pre[i].Conditional || c.Pre[i].Agent != "" || c.Pre[i].Model != "" {
			return fmt.Errorf("loop config: phase[name %s]: conditional, agent, and model are only supported on team members", c.Pre[i].Name)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Pre[i]); err != nil {
			return fmt.Errorf("loop config: phase[name %s]: %w", c.Pre[i].Name, err)
		}
	}

	for i := range c.Loop {
		if c.Loop[i].Name == "" {
			return fmt.Errorf("loop config: phase[index %d]: name must not be empty", i)
		}
		if c.Loop[i].Conditional || c.Loop[i].Agent != "" || c.Loop[i].Model != "" {
			return fmt.Errorf("loop config: phase[name %s]: conditional, agent, and model are only supported on team members", c.Loop[i].Name)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Loop[i]); err != nil {
			return fmt.Errorf("loop config: phase[name %s]: %w", c.Loop[i].Name, err)
		}
	}

	for i := range c.Post {
		if c.Post[i].Name == "" {
			return fmt.Errorf("loop config: phase[index %d]: name must not be empty", i)
		}
		if c.Post[i].Conditional || c.Post[i].Agent != "" || c.Post[i].Model != "" {
			return fmt.Errorf("loop config: phase[name %s]: conditional, agent, and model are only supported on team members", c.Post[i].Name)
		}
		if err := phaseconfig.ValidatePhaseHasPrompt(c.Post[i]); err != nil {
			return fmt.Errorf("loop config: phase[name %s]: %w", c.Post[i].Name, err)
		}
	}

	if err := phaseconfig.ValidatePhaseNames(c.Pre, c.Loop, c.Post); err != nil {
		return fmt.Errorf("loop config: %w", err)
	}

	return nil
}

func (c *LoopConfig) InsertInitialPrompt(prompt string) {
	phaseconfig.InsertInitialPrompt(&c.Pre, prompt)
}

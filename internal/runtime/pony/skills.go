package pony

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Skill is a reusable prompt component loaded from a skill file.
type Skill struct {
	Name           string `json:"name"`
	SystemPrompt   string `json:"system_prompt,omitempty"`
	InitialPrompt  string `json:"initial_prompt,omitempty"`
}

// LoadSkills discovers and loads skills referenced by a profile.
// Discovery order:
//  1. $PONY_SKILLS_DIR env var
//  2. .pony/skills/ next to the config file
//  3. ~/.config/avenor/skills/
func LoadSkills(configDir string, skillNames []string) ([]Skill, error) {
	if len(skillNames) == 0 {
		return nil, nil
	}

	// Build search directories
	dirs := []string{os.Getenv("PONY_SKILLS_DIR")}
	if configDir != "" {
		dirs = append(dirs, filepath.Join(configDir, ".pony", "skills"))
	}
	home, err := os.UserHomeDir()
	if err == nil && home != "" {
		dirs = append(dirs, filepath.Join(home, ".config", "avenor", "skills"))
	}

	// Filter out empty dirs
	var searchDirs []string
	for _, d := range dirs {
		if d != "" {
			searchDirs = append(searchDirs, d)
		}
	}

	skills := make([]Skill, 0, len(skillNames))
	for _, name := range skillNames {
		found := false
		for _, dir := range searchDirs {
			path := filepath.Join(dir, name+".json")
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var skill Skill
			if err := json.Unmarshal(data, &skill); err != nil {
				continue
			}
			skill.Name = name
			skills = append(skills, skill)
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("skill %q not found in any search directory", name)
		}
	}
	return skills, nil
}

// MergeSkills merges skill prompts into a profile's system_prompt and initial_prompt.
// Later skills' prompts are appended with section headers.
func MergeSkills(baseSystem, baseInitial string, skills []Skill) (system, initial string) {
	system = baseSystem
	initial = baseInitial
	for _, s := range skills {
		if s.SystemPrompt != "" {
			if system != "" {
				system += "\n\n"
			}
			system += fmt.Sprintf("## Skill: %s\n%s", s.Name, s.SystemPrompt)
		}
		if s.InitialPrompt != "" {
			if initial != "" {
				initial += "\n\n"
			}
			initial += fmt.Sprintf("## Skill: %s\n%s", s.Name, s.InitialPrompt)
		}
	}
	return system, initial
}

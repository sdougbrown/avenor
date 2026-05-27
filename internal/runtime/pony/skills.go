package pony

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
//
// Skill files can be JSON (.json) or Markdown (.md).
// JSON format: {"system_prompt": "...", "initial_prompt": "..."}
// MD format: first section after frontmatter is system_prompt,
// second section (after ---) is initial_prompt.
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
			skill, err := loadSkillFile(dir, name)
			if err != nil {
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

// loadSkillFile tries to load a skill from a directory, trying .json then .md.
// The resolved path is validated to stay within dir to prevent path traversal.
func loadSkillFile(dir, name string) (Skill, error) {
	// Validate name doesn't contain path separators (prevents path traversal)
	if strings.Contains(name, "/") || strings.Contains(name, "\\") || strings.Contains(name, "..") {
		return Skill{}, fmt.Errorf("invalid skill name %q", name)
	}

	// Try JSON first
	path := filepath.Join(dir, name+".json")
	data, err := os.ReadFile(path)
	if err == nil {
		var skill Skill
		if err := json.Unmarshal(data, &skill); err == nil {
			return skill, nil
		}
	}

	// Try Markdown
	path = filepath.Join(dir, name+".md")
	data, err = os.ReadFile(path)
	if err != nil {
		return Skill{}, err
	}

	return parseSkillMarkdown(name, data)
}

// parseSkillMarkdown parses a markdown skill file.
// Format:
//
//	First section after optional frontmatter → system_prompt
//	Section after --- separator → initial_prompt
func parseSkillMarkdown(name string, data []byte) (Skill, error) {
	content := string(data)
	skill := Skill{Name: name}

	// Split on --- separators
	sections := strings.Split(content, "---")
	
	// Filter out empty sections and strip whitespace
	var parts []string
	for _, s := range sections {
		s = strings.TrimSpace(s)
		if s != "" {
			parts = append(parts, s)
		}
	}

	if len(parts) == 0 {
		return skill, nil
	}

	// First non-empty section is system_prompt
	skill.SystemPrompt = parts[0]

	// Second non-empty section is initial_prompt
	if len(parts) >= 2 {
		skill.InitialPrompt = parts[1]
	}

	return skill, nil
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

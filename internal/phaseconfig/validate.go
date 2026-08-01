package phaseconfig

import "fmt"

func ValidatePhaseNames(phases ...[]Phase) error {
	names := make(map[string]struct{})
	for _, slice := range phases {
		for _, p := range slice {
			if p.Name == "" {
				continue
			}
			if p.Name == "(initial)" {
				return fmt.Errorf("duplicate phase name: \"(initial)\"")
			}
			if _, ok := names[p.Name]; ok {
				return fmt.Errorf("duplicate phase name: %q", p.Name)
			}
			names[p.Name] = struct{}{}
		}
	}
	return nil
}

func ValidatePhaseHasPrompt(p Phase) error {
	if p.Prompt == "" && p.PromptFile == "" && p.LoopFile == "" && p.TeamFile == "" {
		return fmt.Errorf("prompt must not be empty (set prompt, prompt_file, loop_file, or team_file)")
	}
	if p.Prompt != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("prompt is mutually exclusive with loop_file and team_file")
	}
	if p.PromptFile != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("prompt_file is mutually exclusive with loop_file and team_file")
	}
	if p.LoopFile != "" && p.TeamFile != "" {
		return fmt.Errorf("loop_file and team_file are mutually exclusive")
	}
	return nil
}

func ValidatePhaseMutualExclusions(p Phase) error {
	if err := ValidatePhaseRoster(p); err != nil {
		return err
	}
	if p.Prompt != "" && p.PromptFile != "" {
		return fmt.Errorf("phase[name %s]: prompt and prompt_file are mutually exclusive", p.Name)
	}
	if p.Prompt != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("phase[name %s]: prompt is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.PromptFile != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("phase[name %s]: prompt_file is mutually exclusive with loop_file and team_file", p.Name)
	}
	if p.LoopFile != "" && p.TeamFile != "" {
		return fmt.Errorf("phase[name %s]: loop_file and team_file are mutually exclusive", p.Name)
	}
	return nil
}

// ValidatePhaseRoster checks the phase-local roster selector against fields
// that describe a different identity or dispatch a nested workflow.
func ValidatePhaseRoster(p Phase) error {
	if p.RosterEntry != "" && (p.Agent != "" || p.Model != "") {
		return fmt.Errorf("phase[name %s]: roster_entry is mutually exclusive with agent and model", p.Name)
	}
	if p.RosterEntry != "" && (p.LoopFile != "" || p.TeamFile != "") {
		return fmt.Errorf("phase[name %s]: roster_entry is mutually exclusive with loop_file and team_file", p.Name)
	}
	return nil
}

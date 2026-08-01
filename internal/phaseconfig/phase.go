package phaseconfig

type Phase struct {
	Name               string            `json:"name"`
	Prompt             string            `json:"prompt"`
	PromptFile         string            `json:"prompt_file,omitempty"`
	LoopFile           string            `json:"loop_file,omitempty"`
	TeamFile           string            `json:"team_file,omitempty"`
	ResumeFromPrevious bool              `json:"resume_from_previous,omitempty"`
	Conditional        bool              `json:"conditional,omitempty"`
	Agent              string            `json:"agent,omitempty"`
	Model              string            `json:"model,omitempty"`
	RosterEntry        string            `json:"roster_entry,omitempty"`
	Requires           PhaseRequirements `json:"requires,omitempty"`
	OnIncomplete       PhaseOnIncomplete `json:"on_incomplete,omitempty"`
}

type PhaseRequirements struct {
	Files         []string `json:"files,omitempty"`
	NonEmptyFiles []string `json:"non_empty_files,omitempty"`
}

type PhaseOnIncomplete struct {
	Nudge     string `json:"nudge,omitempty"`
	MaxNudges int    `json:"max_nudges,omitempty"`
}

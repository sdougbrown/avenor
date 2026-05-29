package phaseconfig

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
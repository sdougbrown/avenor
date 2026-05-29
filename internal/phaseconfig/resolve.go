package phaseconfig

import (
	"fmt"
	"os"
	"path/filepath"
)

func ResolvePhaseFiles(phases []Phase, configDir string) error {
	for i := range phases {
		p := &phases[i]
		if p.PromptFile == "" {
			continue
		}
		if p.Prompt != "" {
			return fmt.Errorf("phase[name %s]: prompt and prompt_file are mutually exclusive", p.Name)
		}
		absPath := p.PromptFile
		if !filepath.IsAbs(absPath) {
			absPath = filepath.Join(configDir, absPath)
		}
		contents, err := os.ReadFile(absPath)
		if err != nil {
			return fmt.Errorf("phase[name %s]: reading prompt_file %q: %w", p.Name, p.PromptFile, err)
		}
		p.Prompt = string(contents)
		p.PromptFile = ""
	}
	return nil
}
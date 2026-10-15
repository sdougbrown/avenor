package phaseconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// MissingRequiredFiles returns the required output paths that are missing or
// empty in workDir, deduplicated in declaration order.
func MissingRequiredFiles(workDir string, req PhaseRequirements) []string {
	checkedFiles := map[string]struct{}{}
	missingSet := map[string]struct{}{}
	var missing []string
	for _, path := range req.Files {
		if path == "" {
			continue
		}
		if _, ok := checkedFiles[path]; ok {
			continue
		}
		checkedFiles[path] = struct{}{}
		if !requiredFileExists(workDir, path, false) {
			missing = append(missing, path)
			missingSet[path] = struct{}{}
		}
	}
	for _, path := range req.NonEmptyFiles {
		if path == "" {
			continue
		}
		if !requiredFileExists(workDir, path, true) {
			if _, ok := missingSet[path]; !ok {
				missing = append(missing, path)
			}
		}
	}
	return missing
}

func requiredFileExists(workDir, path string, nonEmpty bool) bool {
	fullPath, ok := requiredFilePath(workDir, path)
	if !ok {
		return false
	}
	info, err := os.Stat(fullPath)
	if err != nil || info.IsDir() {
		return false
	}
	return !nonEmpty || info.Size() > 0
}

func requiredFilePath(workDir, path string) (string, bool) {
	if filepath.IsAbs(path) {
		return "", false
	}
	cleanWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return "", false
	}
	cleanWorkDir = filepath.Clean(cleanWorkDir)
	fullPath := filepath.Clean(filepath.Join(cleanWorkDir, path))
	if fullPath != cleanWorkDir && !strings.HasPrefix(fullPath, cleanWorkDir+string(filepath.Separator)) {
		return "", false
	}
	return fullPath, true
}

// IncompleteNudgePrompt renders the follow-up prompt that tells an agent its
// required output files are missing.
func IncompleteNudgePrompt(custom string, missing []string) string {
	if strings.TrimSpace(custom) != "" {
		return custom
	}
	return fmt.Sprintf("Continue. The phase is incomplete because required output files are missing: %s. Complete the assigned task now and write the required files. Do not ask for permission to continue.", strings.Join(missing, ", "))
}

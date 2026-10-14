package stable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LaunchWrapperPolicyFile is the on-disk JSON schema for the MCP launch
// wrapper allowlist:
//
//	{"allowed_argv": [["/usr/bin/nice", "-n", "5"]]}
type LaunchWrapperPolicyFile struct {
	AllowedArgv [][]string `json:"allowed_argv"`
}

// MCPLaunchWrapperPolicy is the in-memory MCP launch wrapper policy. A nil
// policy means launch_wrapper is disabled for MCP-driven spawns on this
// supervisor; a non-nil policy permits only argv arrays that exactly match an
// allowed_argv entry.
type MCPLaunchWrapperPolicy struct {
	AllowedArgv [][]string
}

// LoadMCPLaunchWrapperPolicy reads and validates a policy file strictly:
// unknown fields, trailing data, empty allowlists, empty entries, empty
// elements, NUL bytes, and non-absolute executables are all rejected.
func LoadMCPLaunchWrapperPolicy(path string) (*MCPLaunchWrapperPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var file LaunchWrapperPolicyFile
	if err := dec.Decode(&file); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data after JSON object")
	}
	if len(file.AllowedArgv) == 0 {
		return nil, errors.New("allowed_argv must contain at least one entry")
	}
	policy := &MCPLaunchWrapperPolicy{AllowedArgv: make([][]string, len(file.AllowedArgv))}
	for i, entry := range file.AllowedArgv {
		if len(entry) == 0 {
			return nil, fmt.Errorf("allowed_argv entry %d is empty", i)
		}
		if !filepath.IsAbs(entry[0]) {
			return nil, fmt.Errorf("allowed_argv entry %d executable must be an absolute path: %q", i, entry[0])
		}
		for j, element := range entry {
			if element == "" {
				return nil, fmt.Errorf("allowed_argv entry %d element %d is empty", i, j)
			}
			if strings.ContainsRune(element, '\x00') {
				return nil, fmt.Errorf("allowed_argv entry %d element %d contains a NUL byte", i, j)
			}
		}
		policy.AllowedArgv[i] = entry
	}
	return policy, nil
}

// mcpLaunchWrapperAllowed reports whether wrapper exactly matches one
// allowlist entry.
func mcpLaunchWrapperAllowed(policy *MCPLaunchWrapperPolicy, wrapper []string) bool {
	if policy == nil || len(wrapper) == 0 {
		return false
	}
	for _, allowed := range policy.AllowedArgv {
		if len(allowed) != len(wrapper) {
			continue
		}
		match := true
		for i := range allowed {
			if allowed[i] != wrapper[i] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

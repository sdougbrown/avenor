package poo

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/backendlaunch"
)

// defaultExecutable is the binary a poo entry runs when it names no command.
const defaultExecutable = "poo"

// defaultArgs is the subcommand a poo entry runs when it names no args.
var defaultArgs = []string{"serve"}

func init() {
	backendlaunch.Register(backendID, []string{"command", "args", "settings"}, resolve)
}

// resolve turns a poo roster entry's launch fields into the spec recorded for
// the session. A named command is taken as given and made absolute; without
// one the default executable is looked up on PATH. Args defaults to serve, and
// an explicitly empty args list is honored. The settings path stays as written:
// it is what the recorded argv carries.
func resolve(entryName string, in backendlaunch.Input) (backendlaunch.Spec, error) {
	args := in.Args
	if args == nil {
		args = append([]string(nil), defaultArgs...)
	}
	spec := backendlaunch.Spec{Args: args, Settings: in.Settings}
	if in.Command != "" {
		command, err := filepath.Abs(in.Command)
		if err != nil {
			return backendlaunch.Spec{}, fmt.Errorf("%s: command %q: %w", launchSubject(entryName), in.Command, err)
		}
		spec.Command = command
		return spec, nil
	}
	found, err := exec.LookPath(defaultExecutable)
	if err != nil {
		return backendlaunch.Spec{}, fmt.Errorf("%s: executable %q not found on PATH", launchSubject(entryName), defaultExecutable)
	}
	command, err := filepath.Abs(found)
	if err != nil {
		return backendlaunch.Spec{}, fmt.Errorf("%s: executable %q: %w", launchSubject(entryName), found, err)
	}
	spec.Command = command
	return spec, nil
}

// launchSubject names what a resolution error is about. A direct backend
// selection has no roster entry to name.
func launchSubject(entryName string) string {
	if entryName == "" {
		return fmt.Sprintf("backend %q", backendID)
	}
	return fmt.Sprintf("roster entry %q", entryName)
}

// Argv returns the argv that starts a poo process for spec: the recorded
// command, its args, then --settings when the entry named a settings file.
func Argv(spec backendlaunch.Spec) []string {
	argv := make([]string, 0, 1+len(spec.Args)+2)
	argv = append(argv, spec.Command)
	argv = append(argv, spec.Args...)
	if spec.Settings != "" {
		argv = append(argv, "--settings", spec.Settings)
	}
	return argv
}

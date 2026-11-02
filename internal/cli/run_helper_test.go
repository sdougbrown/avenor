package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/sdougbrown/cli-contract"
)

// run parses args against RunCommand() the way `avenor run` does and returns
// the run's exit code.
func run(args []string, getenv func(string) string, stderr io.Writer) (exitCode int) {
	cmd := RunCommand()
	cmd.Handle = func(context.Context, clicontract.Invocation, io.Writer) (any, error) { return nil, nil }
	inv, err := clicontract.Parse(&clicontract.App{
		Name:        "avenor",
		ErrorPrefix: "avenor",
		Commands:    []*clicontract.Command{cmd},
	}, append([]string{"run"}, args...))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	return RunInvocation(inv, getenv, stderr)
}

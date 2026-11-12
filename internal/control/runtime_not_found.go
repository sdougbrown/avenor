package control

import (
	"errors"
	"fmt"
)

// ErrRuntimeNotFound is the absence of a runtime. The supervisor's HTTP adapter
// returns it directly, and the status verb's typed answer reports it through
// errors.Is. The result, wait_turn, prompt, interrupt_and_prompt and cancel
// lookups return the sentence alone, because no client classifies those
// failures.
var ErrRuntimeNotFound = errors.New("runtime not found")

// CodeRuntimeNotFound is the structured code the status verb attaches when the
// supervisor does not run the requested runtime. A client prefers it to the
// message, which older supervisors still send alone.
const CodeRuntimeNotFound = "runtime_not_found"

// RuntimeNotFoundError is the supervisor's absence answer for the status verb.
// Its message is the sentence clients matched before this code existed, so it
// stays byte-identical, and errors.Is reports ErrRuntimeNotFound, so the status
// verb and the HTTP adapter agree on what one absence is.
type RuntimeNotFoundError struct{ RuntimeID string }

func (e *RuntimeNotFoundError) Error() string {
	return fmt.Sprintf("runtime %q not found", e.RuntimeID)
}

func (e *RuntimeNotFoundError) Is(target error) bool { return target == ErrRuntimeNotFound }

// runtimeNotFound is the control plane's response for a runtime it does not
// run: the sentence older clients match, plus the code and id a newer one reads
// instead of matching it.
func runtimeNotFound(id any, runtimeID string) Response {
	return failure(id, -32000, (&RuntimeNotFoundError{RuntimeID: runtimeID}).Error(), map[string]any{
		"code":       CodeRuntimeNotFound,
		"runtime_id": runtimeID,
	})
}

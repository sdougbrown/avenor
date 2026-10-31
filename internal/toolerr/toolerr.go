// Package toolerr is the typed tool denial every avenor tool surface renders:
// a fixed code, the rendered sentence, and the facts the sentence was built
// from. The messaging and workflow routers share it, so one surface renderer
// handles both and neither router depends on the other.
package toolerr

import (
	"errors"
	"io/fs"
	"strings"
)

// ToolError is a typed tool denial. Code is one of the fixed error codes; the
// message is the rendered template (or, for a refusal, the provider's text).
type ToolError struct {
	Code    string
	Message string
	Facts   map[string]string
}

// ToolCode lets a consumer outside this package classify the error through a
// local interface instead of importing the router that raised it.
func (e *ToolError) ToolCode() string { return e.Code }

func (e *ToolError) Error() string { return e.Message }

// Output is the shared tool result object for a refused or rejected call: only
// the typed code and the rendered text, which carries the same sentence the
// model would see as content.
func (e *ToolError) Output() map[string]any {
	return map[string]any{"code": e.Code, "text": e.Message}
}

// ToolFailure returns the structured object and the rendered sentence a
// surface reports for a router refusal, and false when err is not a
// *ToolError.
func ToolFailure(err error) (object map[string]any, text string, ok bool) {
	var toolErr *ToolError
	if !errors.As(err, &toolErr) {
		return nil, "", false
	}
	return toolErr.Output(), toolErr.Message, true
}

// Refused classifies a transport failure into the typed refused error. A
// filesystem error names an absolute path that must not reach a model or a
// peer, so it is replaced with the caller's shared sentence instead of passed
// through.
func Refused(err error, ioMessage string) *ToolError {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) || errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
		return &ToolError{Code: "refused", Message: ioMessage, Facts: map[string]string{}}
	}
	return &ToolError{Code: "refused", Message: err.Error(), Facts: map[string]string{}}
}

// Substitute renders one template with its facts in a single pass, so a fact
// value that itself contains a placeholder token is never re-substituted and
// the output does not depend on map iteration order.
func Substitute(template string, facts map[string]string) string {
	if template == "" || len(facts) == 0 {
		return template
	}
	var out strings.Builder
	for i := 0; i < len(template); {
		if template[i] == '{' {
			if end := strings.IndexByte(template[i:], '}'); end > 0 {
				name := template[i+1 : i+end]
				if value, ok := facts[name]; ok {
					out.WriteString(value)
					i += end + 1
					continue
				}
			}
		}
		out.WriteByte(template[i])
		i++
	}
	return out.String()
}

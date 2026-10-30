package router

import "github.com/sdougbrown/avenor/internal/toolerr"

// ToolFailure returns the structured object and the rendered sentence a
// surface reports for a router refusal, and false when err is not a
// *ToolError.
func ToolFailure(err error) (object map[string]any, text string, ok bool) {
	return toolerr.ToolFailure(err)
}

package workflow

import "fmt"

// TemplateConflictError reports a StoreTemplate call whose template id and
// version are already registered with different content. Templates are
// immutable revisions: re-registering identical bytes is a no-op, but
// different content under the same id+version is rejected so running
// instances never load a silently replaced template.
type TemplateConflictError struct {
	TemplateID      TemplateID
	TemplateVersion TemplateVersion
}

func (e *TemplateConflictError) Error() string {
	return fmt.Sprintf("template %q version %q already registered with different content", e.TemplateID, e.TemplateVersion)
}

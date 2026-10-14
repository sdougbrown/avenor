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

// TemplateCorruptError reports a stored template file that exists but cannot
// be parsed as a Template, so StoreTemplate cannot determine whether new
// content conflicts with it. The stored file is left untouched.
type TemplateCorruptError struct {
	TemplateID      TemplateID
	TemplateVersion TemplateVersion
	Err             error
}

func (e *TemplateCorruptError) Error() string {
	return fmt.Sprintf("template %q version %q is stored but unreadable: %v", e.TemplateID, e.TemplateVersion, e.Err)
}

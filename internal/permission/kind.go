package permission

import "strings"

// NormalizeOptionKind maps ACP/backend-specific permission kinds onto
// Avenor's decision vocabulary.
func NormalizeOptionKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "allow", "allow_once", "allow_always":
		return "allow"
	case "reject", "reject_once", "reject_always", "deny", "deny_once", "deny_always":
		return "reject"
	default:
		return strings.ToLower(strings.TrimSpace(kind))
	}
}

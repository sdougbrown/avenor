// Package claudecoretest holds the Claude Code screens the claude backend
// tests share, so a reworded screen changes in one place.
package claudecoretest

// ManagedSettingsDialog is Claude Code's managed-settings approval screen, a
// startup dialog avenor does not answer.
const ManagedSettingsDialog = " Managed settings require approval\n\n" +
	" Your organization has configured managed settings that could allow execution of arbitrary code.\n\n" +
	" ❯ 1. Yes, I trust these settings\n" +
	"   2. No, exit Claude Code\n\n" +
	" Enter to confirm · Esc to exit\n"

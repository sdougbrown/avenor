package stable

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/messaging/router"
)

// defaultBranchNames are the branch names a session label does not carry:
// a checkout of the default branch adds nothing the repository name alone
// does not already say.
var defaultBranchNames = map[string]bool{"main": true, "master": true}

// fallbackSessionLabel is the display label a supervisor registers under when
// no work-derived label can be found or every suffixed variant is taken, so
// the supervisor run's id never doubles as its label.
const fallbackSessionLabel = "session"

// maxLabelSuffix is the highest -N collision suffix a label candidate carries,
// and maxLabelSuffixText is that suffix spelled out: the room a truncation
// must leave for one.
const (
	maxLabelSuffix     = 9
	maxLabelSuffixText = "-9"
)

// DefaultSessionLabel derives the default display label for a session working
// in dir: the base name of the git top-level, plus "@<branch>" when the
// checked-out branch is not the repository's default one and HEAD is not
// detached. The label is sanitized to a valid session name — runes no
// identifier may hold are dropped, path separators and whitespace become
// separators, repeats collapse, and the result is truncated to the 64-rune
// name limit — so it passes messaging.ValidSessionName. When the derivation
// yields nothing usable, the sanitized base name of dir is used, and when
// even that fails the label is empty: the caller keeps its fallback label.
// No reserved reference — the empty reference, the self alias, the supervisor
// run id — is ever returned, so a derived label is always reachable by name.
func DefaultSessionLabel(dir string) string {
	if label := sanitizeSessionLabel(derivedWorkLabel(dir)); labelUsable(label) {
		return label
	}
	if label := sanitizeSessionLabel(filepath.Base(dir)); labelUsable(label) {
		return label
	}
	return ""
}

// labelUsable reports whether a sanitized candidate may be presented as a
// session's default display label: it must satisfy the name rule and must not
// be a reference the router resolves before labels are ever consulted.
func labelUsable(label string) bool {
	return messaging.ValidSessionName(label) && !router.IsReservedTarget(label)
}

// pickSessionLabel returns the display label a supervisor run registers under
// for the work-derived base: the base itself when it is free and usable, then
// the base-2 … base-9 suffixed variants, then the fixed fallback label run
// through the same loop. taken reports whether another run already holds a
// label. When even the fallback's suffixes are exhausted, the plain fallback
// label is the last resort: an ambiguous but valid and name-reachable label
// beats one that reads as the supervisor run's id.
func pickSessionLabel(base string, taken func(string) bool) string {
	if label, ok := pickSuffixedSessionLabel(base, taken); ok {
		return label
	}
	if label, ok := pickSuffixedSessionLabel(fallbackSessionLabel, taken); ok {
		return label
	}
	return fallbackSessionLabel
}

// pickSuffixedSessionLabel tries base, then base-2 … base-9, truncating the
// base to leave room for the longest suffix once suffixing starts. It reports
// the first candidate no other run holds that the default-label rules accept;
// a candidate they reject never displaces the fallback path.
func pickSuffixedSessionLabel(base string, taken func(string) bool) (string, bool) {
	candidate := truncateSessionLabel(base, messaging.MaxSessionNameRunes)
	if labelUsable(candidate) && !taken(candidate) {
		return candidate, true
	}
	base = truncateSessionLabel(base, messaging.MaxSessionNameRunes-len(maxLabelSuffixText))
	// A dash at the cut boundary would double with the appended separator.
	base = strings.TrimRight(base, "-")
	if base == "" {
		return "", false
	}
	for n := 2; n <= maxLabelSuffix; n++ {
		candidate = base + "-" + strconv.Itoa(n)
		if labelUsable(candidate) && !taken(candidate) {
			return candidate, true
		}
	}
	return "", false
}

// truncateSessionLabel cuts label to at most limit runes without splitting a
// rune.
func truncateSessionLabel(label string, limit int) string {
	runes := []rune(label)
	if len(runes) > limit {
		runes = runes[:limit]
	}
	return string(runes)
}

// derivedWorkLabel names the work dir carries: the git top-level's base name,
// plus the branch when it is not the default one.
func derivedWorkLabel(dir string) string {
	toplevel, branch, ok := gitWorkRoot(dir)
	if !ok {
		return ""
	}
	label := filepath.Base(toplevel)
	if branch != "" && !defaultBranchNames[branch] {
		label += "@" + branch
	}
	return label
}

// sanitizeSessionLabel turns a derived label into one that can pass
// messaging.ValidSessionName: runes no identifier may hold are dropped under
// messaging.StripDisallowedIdentifierRunes, path separators and whitespace
// runs become a single separator, leading and trailing separators are trimmed,
// and the result is truncated to the 64-rune name limit without splitting a
// rune.
func sanitizeSessionLabel(label string) string {
	label = messaging.StripDisallowedIdentifierRunes(label)
	label = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || unicode.IsSpace(r) {
			return '-'
		}
		return r
	}, label)
	for strings.Contains(label, "--") {
		label = strings.ReplaceAll(label, "--", "-")
	}
	return strings.Trim(truncateSessionLabel(label, messaging.MaxSessionNameRunes), "-")
}

// gitWorkRoot resolves the git working tree dir belongs to: walking up from
// dir for the first .git entry, a directory for a plain checkout and a file
// pointing at the worktree's git directory for a linked worktree. The branch
// is the one .git/HEAD (or the git-directory target's HEAD) names; a detached
// HEAD, or a HEAD that names no branch, yields no branch.
func gitWorkRoot(dir string) (toplevel, branch string, ok bool) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", "", false
	}
	for {
		gitPath := filepath.Join(abs, ".git")
		gitDir := ""
		if info, err := os.Stat(gitPath); err == nil {
			if info.IsDir() {
				gitDir = gitPath
			} else if target := gitDirTarget(gitPath); target != "" {
				gitDir = target
			}
		}
		if gitDir != "" {
			return abs, headBranch(gitDir), true
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", "", false
		}
		abs = parent
	}
}

// gitDirTarget reads a worktree's .git file, whose "gitdir: <path>" line
// points at the git directory the worktree's HEAD lives in.
func gitDirTarget(gitFile string) string {
	data, err := os.ReadFile(gitFile)
	if err != nil {
		return ""
	}
	line := strings.TrimSpace(string(data))
	target, found := strings.CutPrefix(line, "gitdir:")
	if !found {
		return ""
	}
	target = strings.TrimSpace(target)
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(gitFile), target)
	}
	return target
}

// headBranch reads the branch refs/heads/<branch> a git directory's HEAD
// names. A detached HEAD, or any HEAD that does not name a branch, yields no
// branch.
func headBranch(gitDir string) string {
	data, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		return ""
	}
	line := strings.TrimSpace(string(data))
	branch, ok := strings.CutPrefix(line, "ref: refs/heads/")
	if !ok {
		return ""
	}
	return strings.TrimSpace(branch)
}

package stable

import (
	"context"
	"net"
	"net/url"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// defaultBranchNames are the branch names a session label does not carry.
// The repository's real default branch would mean resolving origin/HEAD — a
// config or network read on every derivation — so the derivation treats the
// two conventional names as the default ones instead and carries any other
// branch in the label.
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
// checked-out branch is not one of the conventional default names. The label
// is sanitized to a valid session name — runes no identifier may hold are
// dropped, path separators and whitespace become separators, repeats collapse,
// and the result is truncated to the 64-rune name limit — so it passes
// messaging.ValidSessionName. When the derivation yields nothing usable, the
// sanitized base name of dir is used, and when even that fails the label is
// empty: the caller keeps its fallback label. No reserved reference — the
// empty reference, the self alias, the supervisor run id — is ever returned,
// so a derived label is always reachable by name.
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
	return messaging.ValidSessionName(label)
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
// plus the checked-out branch when it is not one of the conventional default
// names. Both answers come from git itself, so GIT_DIR, GIT_WORK_TREE, bare
// checkouts, linked worktrees and symlinked repositories resolve exactly as
// git sees them; a missing or unreadable repository yields no name at all.
func derivedWorkLabel(dir string) string {
	toplevel := gitOutput(dir, "rev-parse", "--show-toplevel")
	if toplevel == "" {
		return ""
	}
	label := filepath.Base(toplevel)
	if branch := gitOutput(dir, "branch", "--show-current"); branch != "" && !defaultBranchNames[branch] {
		label += "@" + branch
	}
	return label
}

// gitTimeout bounds each git invocation the label derivation makes, so a
// wedged git binary can delay run registration by at most this long.
const gitTimeout = 5 * time.Second

// gitOutput runs git with args in dir and returns its trimmed standard output,
// or "" when git is missing, fails, or exceeds gitTimeout. It is the
// derivation's own instance of the git exec helper shape the other packages
// run their probes through.
func gitOutput(dir string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
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

// shortHost is a machine name without its domain: "grace.home.example.com" is
// "grace".
func shortHost(name string) string {
	short, _, _ := strings.Cut(strings.TrimSpace(name), ".")
	return short
}

// interfaceAddrs lists this machine's own addresses; tests replace it.
var interfaceAddrs = net.InterfaceAddrs

// ownsAddress reports whether ip is one of this machine's interface addresses.
func ownsAddress(ip net.IP) bool {
	addrs, err := interfaceAddrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		switch a := addr.(type) {
		case *net.IPNet:
			if a.IP.Equal(ip) {
				return true
			}
		case *net.IPAddr:
			if a.IP.Equal(ip) {
				return true
			}
		}
	}
	return false
}

// brokerIsLocal reports whether the broker at rawURL runs on this machine: a
// loopback address, one of this machine's own addresses, or the machine's own
// name. An IP literal is never compared by short name. An empty or unparseable
// URL counts as local, so no prefix is added on a guess. The harness label
// defaults (Claude, Pi and Codex) apply the same rule to the broker they
// register on.
func brokerIsLocal(rawURL, hostname string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Hostname() == "" {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ownsAddress(ip)
	}
	return hostname != "" && shortHost(host) == strings.ToLower(shortHost(hostname))
}

// hostPrefixedLabel leads label with the machine's short hostname, sanitized
// and cut to the name limit. When the prefix leaves too little room, the repo
// part of a "<repo>@<branch>" label gives way before the branch does. A
// hostname that yields nothing usable, or an empty label, leaves the label as
// it was.
func hostPrefixedLabel(label, hostname string) string {
	short := sanitizeSessionLabel(shortHost(hostname))
	if short == "" || label == "" {
		return label
	}
	room := messaging.MaxSessionNameRunes - len([]rune(short)) - 1
	if len([]rune(label)) > room {
		if at := strings.LastIndex(label, "@"); at > 0 {
			repo, branch := []rune(label[:at]), []rune(label[at:])
			if repoRoom := room - len(branch); repoRoom > 0 {
				if len(repo) > repoRoom {
					repo = repo[:repoRoom]
				}
				label = string(repo) + string(branch)
			}
		}
	}
	if prefixed := sanitizeSessionLabel(short + "-" + label); labelUsable(prefixed) {
		return prefixed
	}
	return label
}

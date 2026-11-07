package stable

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
)

// gitRepoWithBranch creates a git repository with the named branch checked
// out and returns its path. No commit is made: the label derivation reads
// only the .git layout and HEAD.
func gitRepoWithBranch(t *testing.T, branch string) string {
	t.Helper()
	dir := t.TempDir()
	out, err := exec.Command("git", "-C", dir, "init", "-q", "-b", branch).CombinedOutput()
	if err != nil {
		t.Fatalf("git init -b %s: %v: %s", branch, err, out)
	}
	return dir
}

// writeFakeGitDir plants a .git directory whose only content is a HEAD file,
// enough for the label derivation's git layout reads.
func writeFakeGitDir(t *testing.T, dir, head string) string {
	t.Helper()
	gitDir := filepath.Join(dir, ".git")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatalf("mkdir .git: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte(head), 0o644); err != nil {
		t.Fatalf("write HEAD: %v", err)
	}
	return dir
}

// detachedHead is a HEAD line that names a commit, not a branch.
const detachedHead = "0123456789abcdef0123456789abcdef01234567\n"

func TestSupervisorRegistersWorkDerivedLabel(t *testing.T) {
	repo := gitRepoWithBranch(t, "feature/derived-name")
	sup := newTestSupervisor(t, Config{WorkingDir: repo})
	want := filepath.Base(repo) + "@feature-derived-name"
	if got := sup.broker.DisplayName(messaging.SupervisorRunID); got != want {
		t.Fatalf("supervisor display name = %q, want %q", got, want)
	}
	if matches := sup.broker.MatchLabel(want); len(matches) != 1 || matches[0] != messaging.SupervisorRunID {
		t.Fatalf("derived label resolves to %v, want the supervisor run alone", matches)
	}
}

func TestSupervisorRegistersRepoNameOnDefaultBranch(t *testing.T) {
	repo := gitRepoWithBranch(t, "main")
	sup := newTestSupervisor(t, Config{WorkingDir: repo})
	want := filepath.Base(repo)
	if got := sup.broker.DisplayName(messaging.SupervisorRunID); got != want {
		t.Fatalf("supervisor display name = %q, want %q", got, want)
	}
}

func TestSupervisorFallsBackToDirBaseOutsideARepo(t *testing.T) {
	dir := t.TempDir()
	sup := newTestSupervisor(t, Config{WorkingDir: dir})
	want := filepath.Base(dir)
	if got := sup.broker.DisplayName(messaging.SupervisorRunID); got != want {
		t.Fatalf("supervisor display name = %q, want %q", got, want)
	}
}

// TestSupervisorUsesProcessWorkingDirWithoutConfig pins the empty-field path:
// with no WorkingDir configured the registered label comes from the process
// working directory, and it is a usable display name that never reads as the
// run id.
func TestSupervisorUsesProcessWorkingDirWithoutConfig(t *testing.T) {
	sup := newTestSupervisor(t, Config{})
	label := sup.broker.DisplayName(messaging.SupervisorRunID)
	if !messaging.ValidSessionName(label) {
		t.Fatalf("supervisor display name %q fails the session name rule", label)
	}
	if label == messaging.SupervisorRunID {
		t.Fatalf("supervisor display name is the run id %q", label)
	}
}

// takenLabels builds a taken predicate over a fixed set of held labels, the
// collision seam the pick tests drive.
func takenLabels(labels ...string) func(string) bool {
	held := make(map[string]bool, len(labels))
	for _, label := range labels {
		held[label] = true
	}
	return func(label string) bool { return held[label] }
}

func TestPickSessionLabelSuffixesOnCollision(t *testing.T) {
	got := pickSessionLabel("repo@feat", takenLabels("repo@feat"))
	if got != "repo@feat-2" {
		t.Fatalf("label under collision = %q, want %q", got, "repo@feat-2")
	}
}

func TestPickSessionLabelExhaustedSuffixesTakesFallback(t *testing.T) {
	held := []string{"repo@feat", "repo@feat-2", "repo@feat-3", "repo@feat-4",
		"repo@feat-5", "repo@feat-6", "repo@feat-7", "repo@feat-8", "repo@feat-9"}
	if got := pickSessionLabel("repo@feat", takenLabels(held...)); got != fallbackSessionLabel {
		t.Fatalf("label with every suffix taken = %q, want %q", got, fallbackSessionLabel)
	}
	// An exhausted fallback is still the plain fallback label: the last resort
	// is an ambiguous but valid, name-reachable label.
	held = append(held, fallbackSessionLabel)
	for n := 2; n <= maxLabelSuffix; n++ {
		held = append(held, fallbackSessionLabel+"-"+strconv.Itoa(n))
	}
	if got := pickSessionLabel("repo@feat", takenLabels(held...)); got != fallbackSessionLabel {
		t.Fatalf("label with the fallback exhausted too = %q, want %q", got, fallbackSessionLabel)
	}
}

func TestPickSessionLabelLeavesRoomForTheLongestSuffix(t *testing.T) {
	base := strings.Repeat("r", messaging.MaxSessionNameRunes)
	// No collision: the full-length base survives untouched.
	if got := pickSessionLabel(base, takenLabels()); got != base {
		t.Fatalf("uncollided label = %d runes, want the base unchanged", len([]rune(got)))
	}
	// Collision: the suffix fits inside the name limit.
	got := pickSessionLabel(base, takenLabels(base))
	want := strings.Repeat("r", messaging.MaxSessionNameRunes-len(maxLabelSuffixText)) + "-2"
	if got != want {
		t.Fatalf("collided label = %q, want %q", got, want)
	}
	if !messaging.ValidSessionName(got) {
		t.Fatalf("collided label %q fails the session name rule", got)
	}
	// A dash at the cut boundary is trimmed before the separator is appended,
	// so the suffixed label never carries a doubled separator.
	dashed := strings.Repeat("r", messaging.MaxSessionNameRunes-3) + "-" + "rr"
	got = pickSessionLabel(dashed, takenLabels(dashed))
	want = strings.Repeat("r", messaging.MaxSessionNameRunes-3) + "-2"
	if got != want {
		t.Fatalf("collided label with a dash at the cut = %q, want %q", got, want)
	}
	if strings.Contains(got, "--") {
		t.Fatalf("collided label %q carries a doubled separator", got)
	}
}

func TestPickSessionLabelNeverReturnsAReservedReference(t *testing.T) {
	// A derived base naming a reference the router substitutes before labels
	// are consulted is suffixed instead, so the label stays reachable by name.
	for _, reserved := range []string{"self", messaging.SupervisorRunID} {
		got := pickSessionLabel(reserved, takenLabels())
		if got != reserved+"-2" {
			t.Fatalf("label for reserved base %q = %q, want %q", reserved, got, reserved+"-2")
		}
	}
	if got := pickSessionLabel("", takenLabels()); got != fallbackSessionLabel {
		t.Fatalf("label for an empty base = %q, want %q", got, fallbackSessionLabel)
	}
}

func TestDefaultSessionLabelExcludesReservedReferences(t *testing.T) {
	// A repository whose every derived candidate is a reserved reference yields
	// no default label, so the caller keeps its fallback.
	repo := writeFakeGitDir(t, filepath.Join(t.TempDir(), "self"), "ref: refs/heads/main\n")
	if got := DefaultSessionLabel(repo); got != "" {
		t.Fatalf("label = %q, want the caller to keep its fallback label", got)
	}
}

func TestDefaultSessionLabelDetachedHeadOmitsBranch(t *testing.T) {
	repo := writeFakeGitDir(t, t.TempDir(), detachedHead)
	want := filepath.Base(repo)
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestDefaultSessionLabelRepoOnDefaultBranchOmitsBranch(t *testing.T) {
	repo := writeFakeGitDir(t, t.TempDir(), "ref: refs/heads/master\n")
	want := filepath.Base(repo)
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestDefaultSessionLabelWorktreeGitFile(t *testing.T) {
	main := writeFakeGitDir(t, t.TempDir(), "ref: refs/heads/main\n")
	gitDir := filepath.Join(main, ".git", "worktrees", "linked")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatalf("mkdir worktree git dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte("ref: refs/heads/feature/linked\n"), 0o644); err != nil {
		t.Fatalf("write worktree HEAD: %v", err)
	}
	worktree := filepath.Join(t.TempDir(), "linked-checkout")
	if err := os.MkdirAll(worktree, 0o755); err != nil {
		t.Fatalf("mkdir worktree: %v", err)
	}
	gitFile := "gitdir: " + gitDir + "\n"
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte(gitFile), 0o644); err != nil {
		t.Fatalf("write .git file: %v", err)
	}
	want := filepath.Base(worktree) + "@feature-linked"
	if got := DefaultSessionLabel(worktree); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestDefaultSessionLabelNonRepoFallsBackToDirBase(t *testing.T) {
	dir := t.TempDir()
	if got := DefaultSessionLabel(dir); got != filepath.Base(dir) {
		t.Fatalf("label = %q, want %q", got, filepath.Base(dir))
	}
}

func TestDefaultSessionLabelSanitizesBranchName(t *testing.T) {
	repo := writeFakeGitDir(t, t.TempDir(), "ref: refs/heads/feat/my work\n")
	want := filepath.Base(repo) + "@feat-my-work"
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestDefaultSessionLabelTruncatesToTheNameLimit(t *testing.T) {
	base := strings.Repeat("r", 49)
	repo := writeFakeGitDir(t, filepath.Join(t.TempDir(), base), "ref: refs/heads/"+strings.Repeat("b", 13)+" tail\n")
	want := base + "@" + strings.Repeat("b", 13)
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
	if got := []rune(DefaultSessionLabel(repo)); len(got) > messaging.MaxSessionNameRunes {
		t.Fatalf("label carries %d runes", len(got))
	}
}

func TestDefaultSessionLabelNeverSupervisor(t *testing.T) {
	repo := writeFakeGitDir(t, filepath.Join(t.TempDir(), "supervisor"), "ref: refs/heads/main\n")
	if got := DefaultSessionLabel(repo); got != "" {
		t.Fatalf("label = %q, want the caller to keep its fallback label", got)
	}
}

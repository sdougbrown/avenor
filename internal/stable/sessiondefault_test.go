package stable

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging"
	"github.com/sdougbrown/avenor/internal/workflow"
)

// gitRepoWithBranch creates a git repository with the named branch checked
// out and returns its path. No commit is made: the label derivation reads
// only the checked-out branch and the top-level path.
func gitRepoWithBranch(t *testing.T, branch string) string {
	t.Helper()
	dir := t.TempDir()
	out, err := exec.Command("git", "-C", dir, "init", "-q", "-b", branch).CombinedOutput()
	if err != nil {
		t.Fatalf("git init -b %s: %v: %s", branch, err, out)
	}
	return dir
}

// gitCommit records one empty seed commit, so the repository has a HEAD a
// checkout can detach from.
func gitCommit(t *testing.T, dir string) {
	t.Helper()
	out, err := exec.Command("git", "-C", dir, "-c", "user.email=test@example.com",
		"-c", "user.name=test", "commit", "--allow-empty", "-q", "-m", "seed").CombinedOutput()
	if err != nil {
		t.Fatalf("git commit: %v: %s", err, out)
	}
}

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

// TestSupervisorRegisteredLabelSuffixesOnBrokerCollision drives the real
// collision seam: another run on the supervisor's own broker wears the label
// the working directory derives, and the pick must suffix — through
// broker.MatchLabel and the own-run exclusion, not a fake predicate.
func TestSupervisorRegisteredLabelSuffixesOnBrokerCollision(t *testing.T) {
	repo := gitRepoWithBranch(t, "feature/shared")
	sup := newTestSupervisor(t, Config{WorkingDir: repo})
	base := filepath.Base(repo) + "@feature-shared"
	// No collision at registration: the supervisor wears the derived label.
	if got := sup.broker.DisplayName(messaging.SupervisorRunID); got != base {
		t.Fatalf("supervisor display name = %q, want %q", got, base)
	}
	// Another run takes the label; the broker now resolves it to both runs.
	if _, _, err := sup.broker.EnsureRun("rt_peer", base); err != nil {
		t.Fatalf("seed peer run: %v", err)
	}
	matches := sup.broker.MatchLabel(base)
	if len(matches) != 2 || matches[0] != "rt_peer" || matches[1] != messaging.SupervisorRunID {
		t.Fatalf("MatchLabel(%q) = %v, want rt_peer and the supervisor run", base, matches)
	}
	// The pick seam excludes the supervisor's own run, so the label a supervisor
	// registering in this broker now would take is the first free suffix.
	if !sup.labelHeldByAnotherRun(base) {
		t.Fatal("labelHeldByAnotherRun = false, want the peer's label held")
	}
	if got := pickSessionLabel(DefaultSessionLabel(repo), sup.labelHeldByAnotherRun); got != base+"-2" {
		t.Fatalf("label under broker collision = %q, want %q", got, base+"-2")
	}
}

// TestDefaultSessionLabelSubdirCarriesRootName pins the usual supervisor
// launch layout: the working directory sits below the repository root, and
// the label names the root and its branch.
func TestDefaultSessionLabelSubdirCarriesRootName(t *testing.T) {
	repo := gitRepoWithBranch(t, "feature/derived-name")
	sub := filepath.Join(repo, "sub", "dir")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("mkdir subdir: %v", err)
	}
	want := filepath.Base(repo) + "@feature-derived-name"
	if got := DefaultSessionLabel(sub); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

// TestDefaultSessionLabelDropsZeroWidthSpaceFromBranch pins the rune rule on
// a real branch name: the zero-width space is dropped and the label passes
// the name rule with the @branch suffix intact.
func TestDefaultSessionLabelDropsZeroWidthSpaceFromBranch(t *testing.T) {
	repo := gitRepoWithBranch(t, "feat/\u200bwide")
	label := DefaultSessionLabel(repo)
	want := filepath.Base(repo) + "@feat-wide"
	if label != want {
		t.Fatalf("label = %q, want %q", label, want)
	}
	if !messaging.ValidSessionName(label) {
		t.Fatalf("label %q fails the session name rule", label)
	}
}

func TestDefaultSessionLabelSanitizesBranchName(t *testing.T) {
	repo := gitRepoWithBranch(t, "feat/deep/name")
	want := filepath.Base(repo) + "@feat-deep-name"
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

func TestDefaultSessionLabelTruncatesToTheNameLimit(t *testing.T) {
	repo := gitRepoWithBranch(t, strings.Repeat("b", 80))
	prefix := filepath.Base(repo) + "@"
	want := prefix + strings.Repeat("b", messaging.MaxSessionNameRunes-len([]rune(prefix)))
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
	if got := []rune(DefaultSessionLabel(repo)); len(got) != messaging.MaxSessionNameRunes {
		t.Fatalf("label carries %d runes, want %d", len(got), messaging.MaxSessionNameRunes)
	}
}

func TestDefaultSessionLabelDetachedHeadOmitsBranch(t *testing.T) {
	repo := gitRepoWithBranch(t, "feature/derived-name")
	gitCommit(t, repo)
	out, err := exec.Command("git", "-C", repo, "checkout", "-q", "--detach", "HEAD").CombinedOutput()
	if err != nil {
		t.Fatalf("git checkout --detach: %v: %s", err, out)
	}
	want := filepath.Base(repo)
	if got := DefaultSessionLabel(repo); got != want {
		t.Fatalf("label = %q, want %q", got, want)
	}
}

// TestDefaultSessionLabelWorktreeCarriesWorktreeName pins the linked-worktree
// layout: the top-level is the worktree checkout and the branch is its own.
func TestDefaultSessionLabelWorktreeCarriesWorktreeName(t *testing.T) {
	repo := gitRepoWithBranch(t, "main")
	gitCommit(t, repo)
	worktree := filepath.Join(t.TempDir(), "linked-checkout")
	out, err := exec.Command("git", "-C", repo, "worktree", "add", "-q", "-b",
		"feature/linked", worktree).CombinedOutput()
	if err != nil {
		t.Fatalf("git worktree add: %v: %s", err, out)
	}
	want := "linked-checkout" + "@feature-linked"
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

func TestDefaultSessionLabelNeverSupervisor(t *testing.T) {
	repo := gitRepoWithBranch(t, "main")
	// The repository root itself is named supervisor: the derived label would
	// read as the run id, so the caller keeps its fallback label.
	root := filepath.Join(filepath.Dir(repo), "supervisor")
	if err := os.Rename(repo, root); err != nil {
		t.Fatalf("rename repo root: %v", err)
	}
	if got := DefaultSessionLabel(root); got != "" {
		t.Fatalf("label = %q, want the caller to keep its fallback label", got)
	}
}

func TestDefaultSessionLabelExcludesReservedReferences(t *testing.T) {
	repo := gitRepoWithBranch(t, "main")
	root := filepath.Join(filepath.Dir(repo), "self")
	if err := os.Rename(repo, root); err != nil {
		t.Fatalf("rename repo root: %v", err)
	}
	if got := DefaultSessionLabel(root); got != "" {
		t.Fatalf("label = %q, want the caller to keep its fallback label", got)
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

// TestAttemptSpawnDirSharesTheSupervisorWorkingDir pins the one answer to the
// working-directory question: a workflow attempt with no declared directory
// spawns where the supervisor's label is derived from, so the two cannot
// diverge when the configured directory is set.
func TestAttemptSpawnDirSharesTheSupervisorWorkingDir(t *testing.T) {
	dir := t.TempDir()
	sup := newTestSupervisor(t, Config{WorkingDir: dir})
	got, err := sup.attemptSpawnDir(workflow.ExecutorContext{})
	if err != nil {
		t.Fatalf("attempt spawn dir: %v", err)
	}
	if got != dir {
		t.Fatalf("attempt spawn dir = %q, want the configured working directory %q", got, dir)
	}
}

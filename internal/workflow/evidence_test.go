package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Helper functions
func writeTestFile(t *testing.T, content string) string {
	f := filepath.Join(t.TempDir(), "evidence_file.txt")
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatalf("writeTestFile: %v", err)
	}
	return f
}

func sha256Hex(s string) string {
	digest := sha256.Sum256([]byte(s))
	return hex.EncodeToString(digest[:])
}

func TestStageEvidence_CopiesIntoItsOwnFile(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "same fs content")

	got, err := s.StageEvidence("wf1", src, "out.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence failed: %v", err)
	}
	if got.StoredPath != filepath.Join("evidence", string(got.EvidenceID), "out.txt") {
		t.Fatalf("StoredPath mismatch: got %s", got.StoredPath)
	}

	storedPath := filepath.Join(s.Root(), "instances", "wf1", got.StoredPath)
	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatalf("stat src: %v", err)
	}
	dstInfo, err := os.Stat(storedPath)
	if err != nil {
		t.Fatalf("stat stored: %v", err)
	}
	if os.SameFile(srcInfo, dstInfo) {
		t.Fatal("staged evidence shares its source's inode, want an independent copy")
	}
}

func TestStageEvidence_MissingSource(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, err := s.StageEvidence("wf1", filepath.Join(t.TempDir(), "nope.txt"), "out.txt", false, "")
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	// Verify no workflow.json and no events.ndjson under s.Root()/instances/wf1
	instanceDir := filepath.Join(s.Root(), "instances", "wf1")
	if _, err := os.Stat(filepath.Join(instanceDir, "workflow.json")); err == nil {
		t.Fatal("workflow.json exists but should not")
	}
	if _, err := os.Stat(filepath.Join(instanceDir, "events.ndjson")); err == nil {
		t.Fatal("events.ndjson exists but should not")
	}
}

func TestStageEvidence_EmptyRequiredEmptyFile(t *testing.T) {
	t.Parallel()
	src := writeTestFile(t, "")
	s := newStore(t)
	// required=true should error
	_, err := s.StageEvidence("wf1", src, "out.txt", true, "")
	if err == nil {
		t.Fatal("expected error for empty required file")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error does not contain 'empty': %v", err)
	}

	// required=false should succeed
	got, err := s.StageEvidence("wf1", src, "out.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence with required=false failed: %v", err)
	}
	if got.Size != 0 {
		t.Fatalf("size should be 0, got %d", got.Size)
	}
	if got.SHA256 != sha256Hex("") {
		t.Fatalf("SHA256 mismatch: got %s, want %s", got.SHA256, sha256Hex(""))
	}
}

func TestStageEvidence_Sha256Mismatch(t *testing.T) {
	t.Parallel()
	src := writeTestFile(t, "abc")
	wrong := strings.Repeat("0", 64)
	s := newStore(t)
	// expectedSHA256 wrong should error
	_, err := s.StageEvidence("wf1", src, "out.txt", false, wrong)
	if err == nil {
		t.Fatal("expected error for SHA256 mismatch")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("error does not contain 'mismatch': %v", err)
	}
	// No stored file should exist anywhere under the instance evidence subtree.
	instanceDir := filepath.Join(s.Root(), "instances", "wf1")
	rootEv := filepath.Join(instanceDir, "evidence")
	var unexpected []string
	walkErr := filepath.Walk(rootEv, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// The evidence subtree need not exist yet; a missing root is empty.
			return nil
		}
		if !info.IsDir() {
			unexpected = append(unexpected, path)
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk evidence subtree: %v", walkErr)
	}
	if len(unexpected) > 0 {
		t.Fatalf("expected no stored files after SHA256 mismatch, found %v", unexpected)
	}

	// Correct expectedSHA256 should succeed
	_, err = s.StageEvidence("wf1", src, "out.txt", false, sha256Hex("abc"))
	if err != nil {
		t.Fatalf("StageEvidence with correct SHA256 failed: %v", err)
	}
}

func TestStageEvidence_Copies(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "fallback content")

	got, err := s.StageEvidence("wf1", src, "out.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence with copy fallback failed: %v", err)
	}
	if got.SHA256 != sha256Hex("fallback content") {
		t.Fatalf("SHA256 mismatch: got %s, want %s", got.SHA256, sha256Hex("fallback content"))
	}
	if got.Size != int64(len("fallback content")) {
		t.Fatalf("size mismatch: got %d, want %d", got.Size, len("fallback content"))
	}

	storedPath := filepath.Join(s.Root(), "instances", "wf1", got.StoredPath)
	storedData, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored file: %v", err)
	}
	if string(storedData) != "fallback content" {
		t.Fatalf("stored content mismatch: got %q, want %q", string(storedData), "fallback content")
	}
}

func TestStageEvidence_ImmutabilityDedupeAndFails(t *testing.T) {
	t.Parallel()
	destDir := filepath.Join(t.TempDir(), "evidence")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("mkdir destDir: %v", err)
	}
	// i) First stage
	a := writeTestFile(t, "same content")
	_, _, err := stageInto(a, destDir, "out.txt", false, "", "", nil)
	if err != nil {
		t.Fatalf("stageInto first call failed: %v", err)
	}
	// ii) Dedupe identical content
	b := writeTestFile(t, "same content")
	size, sig, err := stageInto(b, destDir, "out.txt", false, "", "", nil)
	if err != nil {
		t.Fatalf("stageInto second call (dedupe) failed: %v", err)
	}
	if sig != sha256Hex("same content") {
		t.Fatalf("dedupe SHA256 mismatch: got %s, want %s", sig, sha256Hex("same content"))
	}
	if size != int64(len("same content")) {
		t.Fatalf("dedupe size mismatch: got %d, want %d", size, len("same content"))
	}
	// Verify stored file still has original content
	storedPath := filepath.Join(destDir, "out.txt")
	storedData, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored file after dedupe: %v", err)
	}
	if string(storedData) != "same content" {
		t.Fatalf("stored content changed after dedupe: got %q, want %q", string(storedData), "same content")
	}
	// iii) Different content should fail
	c := writeTestFile(t, "DIFFERENT")
	_, _, err = stageInto(c, destDir, "out.txt", false, "", "", nil)
	if err == nil {
		t.Fatal("stageInto with different content should have failed")
	}
	if !strings.Contains(err.Error(), "different content") {
		t.Fatalf("error does not contain 'different content': %v", err)
	}
	// Verify original stored file still has original content (never overwritten)
	storedData2, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored file after failure: %v", err)
	}
	if string(storedData2) != "same content" {
		t.Fatalf("stored content changed on failed overwrite attempt: got %q, want %q", string(storedData2), "same content")
	}
}

func TestStageEvidence_PathEscape(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "x")
	badNames := []string{"../escape.txt", "/abs", "a/../b", "..\\evil"}
	for _, bad := range badNames {
		_, err := s.StageEvidence("wf1", src, bad, false, "")
		if err == nil {
			t.Fatalf("expected error for bad name %q", bad)
		}
	}

	// No file may be staged for any rejected name.
	rootEv := filepath.Join(s.Root(), "instances", "wf1", "evidence")
	var unexpected []string
	_ = filepath.Walk(rootEv, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			unexpected = append(unexpected, path)
		}
		return nil
	})
	if len(unexpected) > 0 {
		t.Fatalf("path-escaping name staged a file: %v", unexpected)
	}
}

func TestStageEvidence_DistinctDirs(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "x")
	g1, err := s.StageEvidence("wf1", src, "a.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence first failed: %v", err)
	}
	g2, err := s.StageEvidence("wf1", src, "b.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence second failed: %v", err)
	}
	if g1.EvidenceID == g2.EvidenceID {
		t.Fatal("EvidenceIDs should be distinct")
	}
	instDir := filepath.Join(s.Root(), "instances", "wf1")
	if _, err := os.Stat(filepath.Join(instDir, "evidence", string(g1.EvidenceID))); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("evidence dir for g1 does not exist")
		}
		t.Fatalf("error checking evidence dir for g1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(instDir, "evidence", string(g2.EvidenceID))); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("evidence dir for g2 does not exist")
		}
		t.Fatalf("error checking evidence dir for g2: %v", err)
	}
}

func TestStageEvidence_LeavesStoreUnchanged(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "x")
	_, err := s.StageEvidence("wf1", src, "out.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence failed: %v", err)
	}
	// Attempt with empty stored name should error
	_, err = s.StageEvidence("wf1", src, "", false, "")
	if err == nil {
		t.Fatal("expected error for empty stored name")
	}
	// After both attempts, workflow.json and events.ndjson should NOT exist
	instDir := filepath.Join(s.Root(), "instances", "wf1")
	if _, err := os.Stat(filepath.Join(instDir, "workflow.json")); err == nil {
		t.Fatal("workflow.json exists but should not (store unchanged)")
	}
	if _, err := os.Stat(filepath.Join(instDir, "events.ndjson")); err == nil {
		t.Fatal("events.ndjson exists but should not (store unchanged)")
	}
}

func TestStageEvidence_NestedPath(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "nested payload")
	got, err := s.StageEvidence("wf1", src, "logs/out.txt", false, "")
	if err != nil {
		t.Fatalf("nested stage failed: %v", err)
	}
	want := filepath.Join("evidence", string(got.EvidenceID), "logs", "out.txt")
	if got.StoredPath != want {
		t.Fatalf("StoredPath = %q, want %q", got.StoredPath, want)
	}
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", "wf1", got.StoredPath))
	if err != nil {
		t.Fatalf("read nested stored file: %v", err)
	}
	if string(stored) != "nested payload" {
		t.Fatalf("content = %q", string(stored))
	}
}

func TestStageEvidence_CopyFailure(t *testing.T) {
	t.Parallel()
	blocker := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	src := writeTestFile(t, "content that cannot land")
	// destDir sits under a path component that is a regular file, so the
	// destination cannot be created and staging must fail with an error.
	if _, _, err := stageInto(src, filepath.Join(blocker, "evidence"), "out.txt", false, "", "", nil); err == nil {
		t.Fatal("expected copy failure for unwritable destination")
	}
}

// stagedEvidenceNames lists the evidence directories an instance holds, so a
// test can assert that a staging attempt left nothing behind.
func stagedEvidenceNames(t *testing.T, s *Store, wf WorkflowID) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(s.instanceDir(wf), "evidence"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatalf("read evidence root: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// TestStageEvidenceFsyncsEveryCreatedDirectoryEntry proves a staging makes each
// directory entry it creates durable, not only the file's bytes. The entries are
// the leaf holding the copy, every directory it created, and the pre-existing
// parent that holds the shallowest one, deepest first. Without the parent sync a
// crash could still lose the evidence directory a just-appended event references.
func TestStageEvidenceFsyncsEveryCreatedDirectoryEntry(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "workflow-root")
	s := New(root)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	var synced []string
	s.evidenceFsync = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}

	staged, err := s.StageEvidence("wf-fsync", writeTestFile(t, "check output\n"), "completion-command.stdout", false, "")
	if err != nil {
		t.Fatalf("StageEvidence: %v", err)
	}
	instanceDir := filepath.Join(root, "instances", "wf-fsync")
	leafDir := filepath.Join(instanceDir, "evidence", string(staged.EvidenceID))
	// The first staging into a fresh root creates the instance and its evidence
	// directory. All three entries are then made durable, deepest first, ending at
	// the pre-existing instances directory that holds them.
	want := []string{leafDir, filepath.Join(instanceDir, "evidence"), instanceDir, filepath.Join(root, "instances")}
	if len(synced) != len(want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Fatalf("synced directories = %v, want %v (deepest first, ending at the created root)", synced, want)
		}
	}
}

// TestStageEvidenceFsyncsAncestorsWhenParentsExist covers staging into an
// existing evidence tree.
func TestStageEvidenceFsyncsAncestorsWhenParentsExist(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if _, err := s.StageEvidence("wf-fsync", writeTestFile(t, "first\n"), "a.txt", false, ""); err != nil {
		t.Fatalf("first staging: %v", err)
	}
	var synced []string
	s.evidenceFsync = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	staged, err := s.StageEvidence("wf-fsync", writeTestFile(t, "second\n"), "b.txt", false, "")
	if err != nil {
		t.Fatalf("second staging: %v", err)
	}
	leafDir := filepath.Join(s.Root(), "instances", "wf-fsync", "evidence", string(staged.EvidenceID))
	instanceDir := filepath.Join(s.Root(), "instances", "wf-fsync")
	// The new leaf, its evidence parent, the instance directory, and the
	// instances directory that holds it: every ancestor is synced on every
	// staging, because an earlier attempt — or a version that never synced the
	// parent — can leave an entry unrecorded.
	want := []string{leafDir, filepath.Join(instanceDir, "evidence"), instanceDir, filepath.Join(s.Root(), "instances")}
	if len(synced) != len(want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Fatalf("synced directories = %v, want %v", synced, want)
		}
	}
}

// TestStageEvidenceSyncsAncestorsAfterAFailedAttempt proves the ancestor syncs
// are not conditional on this call having created them. A staging that fails
// before its own sync leaves the evidence and instance directories on disk but
// unrecorded. The next staging into that instance creates only its own leaf, so
// it must sync those ancestors too, or a crash could lose the directory a
// recorded file lives in.
func TestStageEvidenceSyncsAncestorsAfterAFailedAttempt(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	s.evidenceFsync = func(string) error {
		return errors.New("injected fsync failure")
	}
	if _, err := s.StageEvidence("wf-retry-fsync", writeTestFile(t, "first\n"), "a.txt", false, ""); err == nil {
		t.Fatal("staging with a failing fsync succeeded")
	}
	evidenceRoot := filepath.Join(s.Root(), "instances", "wf-retry-fsync", "evidence")
	entries, err := os.ReadDir(evidenceRoot)
	if err != nil {
		t.Fatalf("read evidence root after the failed staging: %v (the directory must exist to exercise the retry)", err)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence entries after the failed staging = %v, want the failed attempt's directory removed", entries)
	}

	// A later staging creates only its own leaf; the ancestors must still be
	// synced, instance directory included.
	var synced []string
	s.evidenceFsync = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	staged, err := s.StageEvidence("wf-retry-fsync", writeTestFile(t, "second\n"), "b.txt", false, "")
	if err != nil {
		t.Fatalf("retry staging: %v", err)
	}
	instanceDir := filepath.Join(s.Root(), "instances", "wf-retry-fsync")
	leafDir := filepath.Join(evidenceRoot, string(staged.EvidenceID))
	// The instance directory existed before this staging (the interrupted attempt
	// created it) and its own entry in instances/ was never synced: both are
	// synced here.
	want := []string{leafDir, evidenceRoot, instanceDir, filepath.Join(s.Root(), "instances")}
	if len(synced) != len(want) {
		t.Fatalf("synced directories = %v, want %v", synced, want)
	}
	for i := range want {
		if synced[i] != want[i] {
			t.Fatalf("synced directories = %v, want %v", synced, want)
		}
	}
}

// TestStageEvidence_RemovesFreshDirWhenFsyncFails proves a stage that fails
// after its copy landed leaves no orphan behind: the evidence directory it
// minted and the copy inside it are removed, while the artifact an earlier stage
// recorded and the shared evidence parent survive. A later attempt then stages
// normally.
func TestStageEvidence_RemovesFreshDirWhenFsyncFails(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	kept, err := s.StageEvidence("wf-rollback", writeTestFile(t, "kept\n"), "kept.txt", false, "")
	if err != nil {
		t.Fatalf("staging the artifact to keep: %v", err)
	}

	s.evidenceFsync = func(string) error { return errors.New("injected fsync failure") }
	if _, err := s.StageEvidence("wf-rollback", writeTestFile(t, "lost\n"), "lost.txt", false, ""); err == nil {
		t.Fatal("staging with a failing fsync succeeded")
	}

	if got := stagedEvidenceNames(t, s, "wf-rollback"); len(got) != 1 || got[0] != string(kept.EvidenceID) {
		t.Fatalf("evidence directories after the failed staging = %v, want only the kept %s", got, kept.EvidenceID)
	}
	if _, err := os.Stat(filepath.Join(s.Root(), "instances", "wf-rollback", kept.StoredPath)); err != nil {
		t.Fatalf("earlier artifact's copy: %v", err)
	}

	s.evidenceFsync = nil
	retried, err := s.StageEvidence("wf-rollback", writeTestFile(t, "retried\n"), "retried.txt", false, "")
	if err != nil {
		t.Fatalf("retry staging: %v", err)
	}
	if got := stagedEvidenceNames(t, s, "wf-rollback"); len(got) != 2 {
		t.Fatalf("evidence directories after the retry = %v, want the kept and retried ones", got)
	}
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", "wf-rollback", retried.StoredPath))
	if err != nil {
		t.Fatalf("read retried copy: %v", err)
	}
	if string(stored) != "retried\n" {
		t.Fatalf("retried copy = %q, want the retried bytes", stored)
	}
}

// TestStageEvidence_RemovesNestedFreshDirsWhenFsyncFails covers a nested stored
// name, so the rollback also drops the subdirectories one staging created and
// never reaches the directories a sibling instance owns.
func TestStageEvidence_RemovesNestedFreshDirsWhenFsyncFails(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	sibling, err := s.StageEvidence("wf-sibling", writeTestFile(t, "sibling\n"), "out.txt", false, "")
	if err != nil {
		t.Fatalf("sibling staging: %v", err)
	}

	s.evidenceFsync = func(string) error { return errors.New("injected fsync failure") }
	if _, err := s.StageEvidence("wf-rollback", writeTestFile(t, "lost\n"), "logs/lost.txt", false, ""); err == nil {
		t.Fatal("staging with a failing fsync succeeded")
	}

	if got := stagedEvidenceNames(t, s, "wf-rollback"); len(got) != 0 {
		t.Fatalf("evidence directories after the failed staging = %v, want none", got)
	}
	if _, err := os.Stat(filepath.Join(s.Root(), "instances", "wf-rollback", "evidence")); err != nil {
		t.Fatalf("shared evidence parent: %v", err)
	}
	if got := stagedEvidenceNames(t, s, "wf-sibling"); len(got) != 1 || got[0] != string(sibling.EvidenceID) {
		t.Fatalf("sibling instance's evidence = %v, want only %s", got, sibling.EvidenceID)
	}

	s.evidenceFsync = nil
	retried, err := s.StageEvidence("wf-rollback", writeTestFile(t, "retried\n"), "logs/lost.txt", false, "")
	if err != nil {
		t.Fatalf("retry staging: %v", err)
	}
	stored, err := os.ReadFile(filepath.Join(s.Root(), "instances", "wf-rollback", retried.StoredPath))
	if err != nil {
		t.Fatalf("read retried copy: %v", err)
	}
	if string(stored) != "retried\n" {
		t.Fatalf("retried copy = %q, want the retried bytes", stored)
	}
}

// TestStageEvidence_RollsBackWhenAnAncestorSyncFails proves the rollback also
// covers a staging that made its leaf entry durable and then failed on an
// ancestor: the evidence directory goes, and the ancestor directories it shares
// with every other stage stay.
func TestStageEvidence_RollsBackWhenAnAncestorSyncFails(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	instanceDir := filepath.Join(s.Root(), "instances", "wf-rollback")
	s.evidenceFsync = func(dir string) error {
		if dir == instanceDir {
			return errors.New("injected ancestor fsync failure")
		}
		return nil
	}
	if _, err := s.StageEvidence("wf-rollback", writeTestFile(t, "lost\n"), "lost.txt", false, ""); err == nil {
		t.Fatal("staging with a failing ancestor fsync succeeded")
	}

	if got := stagedEvidenceNames(t, s, "wf-rollback"); len(got) != 0 {
		t.Fatalf("evidence directories after the failed staging = %v, want none", got)
	}
	for _, dir := range []string{instanceDir, filepath.Join(s.Root(), "instances")} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("shared ancestor %s: %v", dir, err)
		}
	}
}

// TestStageEvidence_ReportsDiscardFailureWithEvidenceID proves a staging failure
// that also cannot discard its fresh evidence directory reports both: the error
// the caller has to act on keeps its identity, and the copy it could not remove
// is named by its evidence ID instead of being dropped silently.
func TestStageEvidence_ReportsDiscardFailureWithEvidenceID(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	if err := s.CreateRoot(); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	injected := errors.New("injected fsync failure")
	s.evidenceFsync = func(string) error { return injected }
	var discarded []EvidenceID
	s.SetDiscardFault(func(id EvidenceID) error {
		discarded = append(discarded, id)
		return errors.New("injected discard failure")
	})
	t.Cleanup(func() { s.SetDiscardFault(nil) })

	_, err := s.StageEvidence("wf-discard", writeTestFile(t, "captured\n"), "lost.txt", false, "")
	if !errors.Is(err, injected) {
		t.Fatalf("error = %v, want the staging failure's identity preserved", err)
	}
	// Exactly one discard, for the ID this stage minted: a second one would mean
	// two owners of the cleanup rule.
	if len(discarded) != 1 || discarded[0] == "" {
		t.Fatalf("discards = %v, want exactly one for a minted evidence ID", discarded)
	}
	if !strings.Contains(err.Error(), string(discarded[0])) {
		t.Fatalf("error = %v, want it to name the orphaned evidence ID %s", err, discarded[0])
	}
}

// TestStageEvidence_SourceRewriteLeavesEvidenceIntact proves staged evidence
// is independent of its source: a worker that rewrites the source file in
// place after staging (as a later activation in the same working directory
// does) never changes the staged bytes or invalidates the recorded digest.
func TestStageEvidence_SourceRewriteLeavesEvidenceIntact(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "first activation's findings")

	got, err := s.StageEvidence("wf1", src, "correction.md", true, "")
	if err != nil {
		t.Fatalf("StageEvidence: %v", err)
	}
	if err := os.WriteFile(src, []byte("second activation's findings"), 0o600); err != nil {
		t.Fatalf("rewrite source in place: %v", err)
	}

	storedPath := filepath.Join(s.Root(), "instances", "wf1", got.StoredPath)
	storedData, err := os.ReadFile(storedPath)
	if err != nil {
		t.Fatalf("read stored file: %v", err)
	}
	if string(storedData) != "first activation's findings" {
		t.Fatalf("staged evidence changed with its source: got %q", storedData)
	}
	if digest := sha256Hex(string(storedData)); digest != got.SHA256 {
		t.Fatalf("staged evidence digest = %s, want the recorded %s", digest, got.SHA256)
	}
}

// TestStageEvidenceLeavesNoDirectoryWhenStagingFails proves a failed stage
// removes the directory it minted, so no caller has to track a copy it never
// received an ID for.
func TestStageEvidenceLeavesNoDirectoryWhenStagingFails(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	src := writeTestFile(t, "captured\n")
	injected := errors.New("injected sync failure")
	s.evidenceFsync = func(string) error { return injected }
	if _, err := s.StageEvidence("wf1", src, "artifact.json", false, ""); !errors.Is(err, injected) {
		t.Fatalf("error = %v, want the staging failure", err)
	}
	entries, err := os.ReadDir(filepath.Join(s.Root(), "instances", "wf1", "evidence"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("read evidence root: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("evidence entries = %v, want none after a failed stage", entries)
	}
}

// TestStageEvidence_ValidationRejectionIsNotACleanupFailure proves a rejected
// request returns the bare validation error: no evidence directory was minted, so
// there is nothing to discard and nothing to report.
func TestStageEvidence_ValidationRejectionIsNotACleanupFailure(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, err := s.StageEvidence("wf-validate", writeTestFile(t, "captured\n"), "../escape.txt", false, "")
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("error = %v, want the stored-path validation error", err)
	}
	if strings.Contains(err.Error(), "discard orphaned evidence") {
		t.Fatalf("error = %v, want no cleanup clause on a rejected request", err)
	}
}

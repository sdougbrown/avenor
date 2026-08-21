package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

func TestStageEvidence_HardLink(t *testing.T) {
	s := newStore(t)
	src := writeTestFile(t, "hello evidence")
	got, err := s.StageEvidence("wf1", src, "out.txt", false, "")
	if err != nil {
		t.Fatalf("StageEvidence failed: %v", err)
	}
	if got.SHA256 != sha256Hex("hello evidence") {
		t.Fatalf("SHA256 mismatch: got %s, want %s", got.SHA256, sha256Hex("hello evidence"))
	}
	if got.Size != int64(len("hello evidence")) {
		t.Fatalf("size mismatch: got %d, want %d", got.Size, len("hello evidence"))
	}
	if got.StoredPath != filepath.Join("evidence", string(got.EvidenceID), "out.txt") {
		t.Fatalf("StoredPath mismatch: got %s", got.StoredPath)
	}
	srcInfo, err := os.Stat(src)
	if err != nil {
		t.Fatalf("stat src: %v", err)
	}
	storedPath := filepath.Join(s.Root(), "instances", "wf1", got.StoredPath)
	storedInfo, err := os.Stat(storedPath)
	if err != nil {
		t.Fatalf("stat stored: %v", err)
	}
	if !os.SameFile(srcInfo, storedInfo) {
		t.Fatal("hard link failed - files are not the same inode")
	}
}

func TestStageEvidence_MissingSource(t *testing.T) {
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

func TestStageEvidence_CopyFallback(t *testing.T) {
	s := newStore(t)
	src := writeTestFile(t, "fallback content")

	originalLink := evidenceLink
	evidenceLink = func(oldname, newname string) error { return errors.New("cross-device") }
	defer func() { evidenceLink = originalLink }()

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
	destDir := filepath.Join(t.TempDir(), "evidence")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatalf("mkdir destDir: %v", err)
	}
	// i) First stage
	a := writeTestFile(t, "same content")
	_, _, err := stageInto(a, destDir, "out.txt", false, "")
	if err != nil {
		t.Fatalf("stageInto first call failed: %v", err)
	}
	// ii) Dedupe identical content
	b := writeTestFile(t, "same content")
	size, sig, err := stageInto(b, destDir, "out.txt", false, "")
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
	_, _, err = stageInto(c, destDir, "out.txt", false, "")
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
	blocker := filepath.Join(t.TempDir(), "notadir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	src := writeTestFile(t, "content that cannot land")
	// destDir sits under a path component that is a regular file, so the
	// destination cannot be created and staging must fail with an error.
	if _, _, err := stageInto(src, filepath.Join(blocker, "evidence"), "out.txt", false, ""); err == nil {
		t.Fatal("expected copy failure for unwritable destination")
	}
}

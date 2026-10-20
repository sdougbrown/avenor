package durablefile

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

// assertOnlyTarget reports whether dir holds the target file and nothing else,
// so a staged temp file that survived the write fails the test by its presence.
func assertOnlyTarget(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !slices.Equal(names, []string{"target"}) {
		t.Fatalf("dir holds %v, want only the target", names)
	}
}

func seedFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("seed %s: %v", path, err)
	}
}

func TestWriteFileAtomicReplacesTarget(t *testing.T) {
	tests := []struct {
		name string
		perm os.FileMode
	}{
		{name: "0600", perm: 0o600},
		{name: "0644", perm: 0o644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "target")
			seedFile(t, path, "old")
			before, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat before: %v", err)
			}

			if err := WriteFileAtomic(path, TempPattern(path), []byte("new"), tt.perm); err != nil {
				t.Fatalf("WriteFileAtomic: %v", err)
			}

			if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
				t.Fatalf("content = %q (err %v), want %q", got, err, "new")
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat after: %v", err)
			}
			if os.SameFile(before, after) {
				t.Fatal("target was written in place; the commit must replace it")
			}
			if perm := after.Mode().Perm(); perm != tt.perm {
				t.Fatalf("mode = %04o, want %04o", perm, tt.perm)
			}
			assertOnlyTarget(t, dir)
		})
	}
}

func TestWriteFileAtomicExclusiveKeepsExistingTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	seedFile(t, path, "sentinel")

	err := WriteFileAtomicExclusive(path, TempPattern(path), []byte("new"), 0o600)
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("WriteFileAtomicExclusive error = %v, want fs.ErrExist", err)
	}
	if got, readErr := os.ReadFile(path); readErr != nil || string(got) != "sentinel" {
		t.Fatalf("content = %q (err %v), want the untouched sentinel", got, readErr)
	}
	assertOnlyTarget(t, dir)
}

func TestWriteFileAtomicExclusiveAdmitsOneRacer(t *testing.T) {
	const writers = 16
	dir := t.TempDir()
	path := filepath.Join(dir, "target")

	var wg sync.WaitGroup
	payloads := make([]string, writers)
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payloads[i] = fmt.Sprintf("writer-%d", i)
			errs[i] = WriteFileAtomicExclusive(path, TempPattern(path), []byte(payloads[i]), 0o600)
		}(i)
	}
	wg.Wait()

	published := -1
	var existing int
	for i, err := range errs {
		switch {
		case err == nil:
			published = i
		case errors.Is(err, fs.ErrExist):
			existing++
		default:
			t.Errorf("writer %d: unexpected error %v", i, err)
		}
	}
	if published < 0 || existing != writers-1 {
		t.Fatalf("published writer = %d, fs.ErrExist = %d; want one publisher and %d", published, existing, writers-1)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != payloads[published] {
		t.Fatalf("content = %q (err %v), want writer %d's whole payload", got, err, published)
	}
	assertOnlyTarget(t, dir)
}

func TestWriteFileAtomicAdmitsEveryRacer(t *testing.T) {
	const writers = 16
	dir := t.TempDir()
	path := filepath.Join(dir, "target")

	var wg sync.WaitGroup
	payloads := make([]string, writers)
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payloads[i] = fmt.Sprintf("writer-%d", i)
			errs[i] = WriteFileAtomic(path, TempPattern(path), []byte(payloads[i]), 0o600)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if !slices.Contains(payloads, string(got)) {
		t.Fatalf("content = %q, want exactly one writer's payload", got)
	}
	assertOnlyTarget(t, dir)
}

func TestWriteFileAtomicExclusiveReportsPublishedTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	removeErr := errors.New("temp is busy")
	restore := removeTemp
	removeTemp = func(string) error { return removeErr }
	t.Cleanup(func() { removeTemp = restore })

	err := WriteFileAtomicExclusive(path, TempPattern(path), []byte("payload"), 0o600)
	if !errors.Is(err, ErrTempNotRemoved) {
		t.Fatalf("WriteFileAtomicExclusive error = %v, want ErrTempNotRemoved", err)
	}
	if errors.Is(err, fs.ErrExist) {
		t.Fatalf("error = %v, want no fs.ErrExist match: the payload was published", err)
	}
	if !errors.Is(err, removeErr) {
		t.Fatalf("error = %v, want the removal error wrapped", err)
	}
	if got, readErr := os.ReadFile(path); readErr != nil || string(got) != "payload" {
		t.Fatalf("content = %q (err %v), want the published payload", got, readErr)
	}
	var leftover *TempNotRemovedError
	if !errors.As(err, &leftover) {
		t.Fatalf("error = %v, want a *TempNotRemovedError naming the leftover temp", err)
	}
	if _, statErr := os.Stat(leftover.Path); statErr != nil {
		t.Fatalf("leftover temp %s: %v", leftover.Path, statErr)
	}
}

func TestWriteFileAtomicCommitFailureRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	// A directory at the target path makes the rename fail.
	path := filepath.Join(dir, "target")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	if err := WriteFileAtomic(path, TempPattern(path), []byte("new"), 0o600); err == nil {
		t.Fatal("WriteFileAtomic: expected an error when the target is a directory")
	}

	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("target is no longer the directory that made the commit fail (err %v)", err)
	}
	assertOnlyTarget(t, dir)
}

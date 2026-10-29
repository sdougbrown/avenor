package pi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSessionsPersistEnabled(t *testing.T) {
	// The empty-value case: Setenv with "" leaves the variable present but
	// empty. Unset is covered by TestSessionsPersistEnabledUnset.
	cases := []struct {
		value string
		want  bool
	}{
		{"", false},
		{"0", false},
		{"false", false},
		{"no", false},
		{"1", true},
		{"true", true},
		{"YES", true},
		{" on ", true},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(SessionsEnv, tc.value)
			if got := SessionsPersistEnabled(); got != tc.want {
				t.Fatalf("SessionsPersistEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// Unset is a distinct state from the table's empty-value case: Setenv with
// "" leaves the variable present with an empty value, Unsetenv removes it.
func TestSessionsPersistEnabledUnset(t *testing.T) {
	os.Unsetenv(SessionsEnv)
	if SessionsPersistEnabled() {
		t.Fatal("SessionsPersistEnabled() = true with the variable unset")
	}
}

func TestPersistSessionsDirExplicitEnv(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "explicit")
	t.Setenv(SessionDirEnv, dir)
	got, err := PersistSessionsDir()
	if err != nil {
		t.Fatalf("PersistSessionsDir: %v", err)
	}
	if got != dir {
		t.Fatalf("PersistSessionsDir() = %q, want %q", got, dir)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("session dir %s was not created: %v", dir, err)
	}
}

func TestPersistSessionsDirDefaultUnderStateHome(t *testing.T) {
	state := t.TempDir()
	t.Setenv(SessionDirEnv, "")
	t.Setenv("XDG_STATE_HOME", state)
	got, err := PersistSessionsDir()
	if err != nil {
		t.Fatalf("PersistSessionsDir: %v", err)
	}
	want := filepath.Join(state, "avenor", "pi-sessions")
	if got != want {
		t.Fatalf("PersistSessionsDir() = %q, want %q", got, want)
	}
}

func TestSessionFileExists(t *testing.T) {
	dir := t.TempDir()
	if ok, err := SessionFileExists(dir, "01a117d6-a744-7108-89ff-139b1a47b088"); ok || err != nil {
		t.Fatalf("SessionFileExists = %v, %v for an empty directory, want false, nil", ok, err)
	}
	name := "2026-10-14T14-55-09-163Z_01a117d6-a744-7108-89ff-139b1a47b088.jsonl"
	if err := os.WriteFile(filepath.Join(dir, name), []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "other_01a117d6.jsonl"), []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	// A partial ID that matches a prefix-named file is expected suffix
	// behavior; avenor only passes full IDs, so the assertion pins the
	// suffix contract rather than a partial-ID guarantee.
	if ok, err := SessionFileExists(dir, "01a117d6"); !ok || err != nil {
		t.Fatalf("SessionFileExists = %v, %v for a prefix-named session file, want true, nil", ok, err)
	}
	if ok, err := SessionFileExists(dir, "01a117d6-a744-7108-89ff-139b1a47b088"); !ok || err != nil {
		t.Fatalf("SessionFileExists = %v, %v for a matching session file, want true, nil", ok, err)
	}
	if ok, err := SessionFileExists(dir, "ffffff"); ok || err != nil {
		t.Fatalf("SessionFileExists = %v, %v for an ID with no matching file, want false, nil", ok, err)
	}
	_, err := SessionFileExists(filepath.Join(dir, "missing"), "01a117d6-a744-7108-89ff-139b1a47b088")
	if err == nil {
		t.Fatal("SessionFileExists = nil error for an unreadable directory, want the read failure")
	}
}

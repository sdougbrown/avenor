package pi

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSessionsPersistEnabled(t *testing.T) {
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

func TestSessionsPersistEnabledUnset(t *testing.T) {
	t.Setenv(SessionsEnv, "")
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
	if SessionFileExists(dir, "01a117d6-a744-7108-89ff-139b1a47b088") {
		t.Fatal("SessionFileExists = true for an empty directory")
	}
	name := "2026-10-14T14-55-09-163Z_01a117d6-a744-7108-89ff-139b1a47b088.jsonl"
	if err := os.WriteFile(filepath.Join(dir, name), []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "other_01a117d6.jsonl"), []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	if !SessionFileExists(dir, "01a117d6-a744-7108-89ff-139b1a47b088") {
		t.Fatal("SessionFileExists = false for a matching session file")
	}
	if SessionFileExists(dir, "ffffff") {
		t.Fatal("SessionFileExists = true for an ID with no matching file")
	}
	if SessionFileExists(filepath.Join(dir, "missing"), "01a117d6-a744-7108-89ff-139b1a47b088") {
		t.Fatal("SessionFileExists = true for a missing directory")
	}
}

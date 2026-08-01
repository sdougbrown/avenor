package looprunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeLoopRosterFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeLoopConfigFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadLoopConfigWithRosterAcceptsRosterPhasesAndRelativeFile(t *testing.T) {
	dir := t.TempDir()
	writeLoopRosterFile(t, dir, "roster.json", `{"planner":{"backend":"agy","agent":"planner"}}`)
	path := writeLoopConfigFile(t, dir, "loop.json", `{
		"roster_file":"roster.json",
		"pre":[{"name":"pre","prompt":"prepare","roster_entry":"planner"}],
		"loop":[{"name":"loop","prompt":"iterate","roster_entry":"planner"}],
		"post":[{"name":"post","prompt":"report","roster_entry":"planner"}]
	}`)

	cfg, roster, err := LoadLoopConfigWithRoster(path, nil, "")
	if err != nil {
		t.Fatalf("LoadLoopConfigWithRoster() error = %v", err)
	}
	if cfg.Pre[0].RosterEntry != "planner" || cfg.Loop[0].RosterEntry != "planner" || cfg.Post[0].RosterEntry != "planner" {
		t.Fatalf("roster phases were not decoded: %+v", cfg)
	}
	if roster == nil {
		t.Fatal("expected effective roster")
	}
	if _, err := roster.Lookup("planner"); err != nil {
		t.Fatalf("effective roster lookup = %v", err)
	}
}

func TestLoadLoopConfigWithRosterUsesRootFallbackWhenUndeclared(t *testing.T) {
	dir := t.TempDir()
	fallback := writeLoopRosterFile(t, dir, "fallback.json", `{"entry":{"backend":"fallback","agent":"fallback"}}`)
	path := writeLoopConfigFile(t, dir, "loop.json", `{"pre":[{"name":"work","prompt":"work","roster_entry":"entry"}]}`)

	_, roster, err := LoadLoopConfigWithRoster(path, nil, fallback)
	if err != nil {
		t.Fatalf("fallback load = %v", err)
	}
	if got, _ := roster.Lookup("entry"); got.Backend != "fallback" {
		t.Fatalf("fallback roster entry = %+v, want fallback backend", got)
	}
}

func TestLoadLoopConfigWithRosterPrecedenceAndRecursiveReplacement(t *testing.T) {
	dir := t.TempDir()
	fallback := writeLoopRosterFile(t, dir, "fallback.json", `{"entry":{"backend":"fallback","agent":"fallback"}}`)
	writeLoopRosterFile(t, dir, "root-roster.json", `{"entry":{"backend":"root","agent":"root"}}`)
	writeLoopRosterFile(t, dir, "child-roster.json", `{"entry":{"backend":"child","agent":"child"}}`)
	rootPath := writeLoopConfigFile(t, dir, "root.json", `{"roster_file":"root-roster.json","pre":[{"name":"root","prompt":"root","roster_entry":"entry"}]}`)
	childPath := writeLoopConfigFile(t, dir, "child.json", `{"pre":[{"name":"child","prompt":"child","roster_entry":"entry"}]}`)
	replacementPath := writeLoopConfigFile(t, dir, "replacement.json", `{"roster_file":"child-roster.json","pre":[{"name":"child","prompt":"child","roster_entry":"entry"}]}`)
	grandchildPath := writeLoopConfigFile(t, dir, "grandchild.json", `{"pre":[{"name":"grandchild","prompt":"grandchild","roster_entry":"entry"}]}`)

	_, root, err := LoadLoopConfigWithRoster(rootPath, nil, fallback)
	if err != nil {
		t.Fatalf("root load = %v", err)
	}
	if got, _ := root.Lookup("entry"); got.Backend != "root" {
		t.Fatalf("declared root roster lost to fallback: %+v", got)
	}
	_, inherited, err := LoadLoopConfigWithRoster(childPath, root, fallback)
	if err != nil {
		t.Fatalf("inherited child load = %v", err)
	}
	if got, _ := inherited.Lookup("entry"); got.Backend != "root" {
		t.Fatalf("child did not inherit root roster: %+v", got)
	}
	_, replaced, err := LoadLoopConfigWithRoster(replacementPath, root, fallback)
	if err != nil {
		t.Fatalf("replacement child load = %v", err)
	}
	if got, _ := replaced.Lookup("entry"); got.Backend != "child" {
		t.Fatalf("child declaration did not replace inherited roster: %+v", got)
	}
	_, grandchild, err := LoadLoopConfigWithRoster(grandchildPath, replaced, "")
	if err != nil {
		t.Fatalf("grandchild load = %v", err)
	}
	if got, _ := grandchild.Lookup("entry"); got.Backend != "child" {
		t.Fatalf("grandchild did not inherit nearest roster: %+v", got)
	}
}

func TestLoadLoopConfigWithRosterRejectsInvalidRosterReferencesAndCombinations(t *testing.T) {
	dir := t.TempDir()
	writeLoopRosterFile(t, dir, "roster.json", `{"known":{"backend":"agy","agent":"known"}}`)
	tests := []struct {
		name string
		body string
		want string
	}{
		{"unknown entry", `{"roster_file":"roster.json","pre":[{"name":"work","prompt":"work","roster_entry":"missing"}]}`, `roster entry "missing" not found`},
		{"missing roster context", `{"pre":[{"name":"work","prompt":"work","roster_entry":"known"}]}`, "requires a roster file"},
		{"roster plus inline identity", `{"roster_file":"roster.json","pre":[{"name":"work","prompt":"work","roster_entry":"known","agent":"inline"}]}`, "roster_entry is mutually exclusive"},
		{"roster plus nested workflow", `{"roster_file":"roster.json","pre":[{"name":"work","loop_file":"child.json","roster_entry":"known"}]}`, "roster_entry is mutually exclusive with loop_file and team_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeLoopConfigFile(t, dir, tt.name+".json", tt.body)
			_, err := LoadLoopConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadLoopConfig() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoadLoopConfigCompatibilityLoaderStillLoadsDeclaredRoster(t *testing.T) {
	dir := t.TempDir()
	writeLoopRosterFile(t, dir, "roster.json", `{"known":{"backend":"agy","agent":"known"}}`)
	path := writeLoopConfigFile(t, dir, "loop.json", `{"roster_file":"roster.json","pre":[{"name":"work","prompt":"work","roster_entry":"known"}]}`)
	if _, err := LoadLoopConfig(path); err != nil {
		t.Fatalf("compatibility loader rejected declared roster: %v", err)
	}
}

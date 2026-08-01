package teamrunner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTeamRosterFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeTeamConfigFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadTeamConfigWithRosterAcceptsRosterPhasesAndRelativeFile(t *testing.T) {
	dir := t.TempDir()
	writeTeamRosterFile(t, dir, "roster.json", `{"planner":{"backend":"agy","agent":"planner"}}`)
	path := writeTeamConfigFile(t, dir, "team.json", `{
		"roster_file":"roster.json",
		"pre":[{"name":"pre","prompt":"prepare","roster_entry":"planner"}],
		"team":[{"name":"team","prompt":"work","roster_entry":"planner"}],
		"post":[{"name":"post","prompt":"report","roster_entry":"planner"}]
	}`)

	cfg, roster, err := LoadTeamConfigWithRoster(path, nil, "")
	if err != nil {
		t.Fatalf("LoadTeamConfigWithRoster() error = %v", err)
	}
	if cfg.Pre[0].RosterEntry != "planner" || cfg.Team[0].RosterEntry != "planner" || cfg.Post[0].RosterEntry != "planner" {
		t.Fatalf("roster phases were not decoded: %+v", cfg)
	}
	if roster == nil {
		t.Fatal("expected effective roster")
	}
}

func TestLoadTeamConfigWithRosterUsesRootFallbackWhenUndeclared(t *testing.T) {
	dir := t.TempDir()
	fallback := writeTeamRosterFile(t, dir, "fallback.json", `{"entry":{"backend":"fallback","agent":"fallback"}}`)
	path := writeTeamConfigFile(t, dir, "team.json", `{"team":[{"name":"work","prompt":"work","roster_entry":"entry"}]}`)

	_, roster, err := LoadTeamConfigWithRoster(path, nil, fallback)
	if err != nil {
		t.Fatalf("fallback load = %v", err)
	}
	if got, _ := roster.Lookup("entry"); got.Backend != "fallback" {
		t.Fatalf("fallback roster entry = %+v, want fallback backend", got)
	}
}

func TestLoadTeamConfigWithRosterFallbackAndChildReplacement(t *testing.T) {
	dir := t.TempDir()
	fallback := writeTeamRosterFile(t, dir, "fallback.json", `{"entry":{"backend":"fallback","agent":"fallback"}}`)
	writeTeamRosterFile(t, dir, "parent.json", `{"entry":{"backend":"parent","agent":"parent"}}`)
	writeTeamRosterFile(t, dir, "child.json", `{"entry":{"backend":"child","agent":"child"}}`)
	rootPath := writeTeamConfigFile(t, dir, "root.json", `{"roster_file":"parent.json","team":[{"name":"root","prompt":"root","roster_entry":"entry"}]}`)
	childPath := writeTeamConfigFile(t, dir, "child-config.json", `{"team":[{"name":"child","prompt":"child","roster_entry":"entry"}]}`)
	replacementPath := writeTeamConfigFile(t, dir, "replacement.json", `{"roster_file":"child.json","team":[{"name":"child","prompt":"child","roster_entry":"entry"}]}`)
	grandchildPath := writeTeamConfigFile(t, dir, "grandchild.json", `{"team":[{"name":"grandchild","prompt":"grandchild","roster_entry":"entry"}]}`)

	_, root, err := LoadTeamConfigWithRoster(rootPath, nil, fallback)
	if err != nil {
		t.Fatalf("root load = %v", err)
	}
	if got, _ := root.Lookup("entry"); got.Backend != "parent" {
		t.Fatalf("declared roster lost to fallback: %+v", got)
	}
	_, inherited, err := LoadTeamConfigWithRoster(childPath, root, fallback)
	if err != nil {
		t.Fatalf("inherited child load = %v", err)
	}
	if got, _ := inherited.Lookup("entry"); got.Backend != "parent" {
		t.Fatalf("child did not inherit parent roster: %+v", got)
	}
	_, replaced, err := LoadTeamConfigWithRoster(replacementPath, root, fallback)
	if err != nil {
		t.Fatalf("replacement child load = %v", err)
	}
	if got, _ := replaced.Lookup("entry"); got.Backend != "child" {
		t.Fatalf("child declaration did not replace inherited roster: %+v", got)
	}
	_, grandchild, err := LoadTeamConfigWithRoster(grandchildPath, replaced, "")
	if err != nil {
		t.Fatalf("grandchild load = %v", err)
	}
	if got, _ := grandchild.Lookup("entry"); got.Backend != "child" {
		t.Fatalf("grandchild did not inherit nearest roster: %+v", got)
	}
}

func TestLoadTeamConfigWithRosterRejectsInvalidRosterCombinations(t *testing.T) {
	dir := t.TempDir()
	writeTeamRosterFile(t, dir, "roster.json", `{"known":{"backend":"agy","agent":"known"}}`)
	tests := []struct {
		name string
		body string
		want string
	}{
		{"unknown entry", `{"roster_file":"roster.json","team":[{"name":"work","prompt":"work","roster_entry":"missing"}]}`, `roster entry "missing" not found`},
		{"roster plus inline identity", `{"roster_file":"roster.json","team":[{"name":"work","prompt":"work","roster_entry":"known","model":"inline"}]}`, "roster_entry is mutually exclusive"},
		{"roster plus nested workflow", `{"roster_file":"roster.json","post":[{"name":"work","team_file":"child.json","roster_entry":"known"}]}`, "roster_entry is mutually exclusive with loop_file and team_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeTeamConfigFile(t, dir, tt.name+".json", tt.body)
			_, err := LoadTeamConfig(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("LoadTeamConfig() error = %v, want %q", err, tt.want)
			}
		})
	}
}

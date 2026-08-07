package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sdougbrown/avenor/internal/looprunner"
	"github.com/sdougbrown/avenor/internal/phaseconfig"
	"github.com/sdougbrown/avenor/internal/rosterconfig"
	"github.com/sdougbrown/avenor/internal/teamrunner"
)

// runVerify loads and validates config files without starting a run. It
// checks structural correctness (format, unknown fields, mutual exclusions,
// prompt presence, roster entry references) and recursively validates nested
// loop_file/team_file references that are only loaded on demand during a run.
func runVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dir := fs.String("dir", ".", "working directory (config file paths are resolved relative to this)")
	loopFile := fs.String("loop-file", "", "path to loop config to validate")
	teamFile := fs.String("team-file", "", "path to team config to validate")
	rosterFile := fs.String("roster-file", "", "path to roster config to validate (standalone or as fallback for loop/team)")
	rosterEntry := fs.String("roster-entry", "", "roster entry name to look up (requires --roster-file)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *rosterEntry != "" && *rosterFile == "" {
		fmt.Fprintln(os.Stderr, "avenor verify: --roster-entry requires --roster-file")
		return 1
	}

	if *loopFile == "" && *teamFile == "" && *rosterFile == "" {
		fmt.Fprintln(os.Stderr, "avenor verify: at least one of --loop-file, --team-file, or --roster-file is required")
		return 1
	}

	v := newVerifier(*dir)
	v.addRoster(*rosterFile, *rosterEntry)
	v.addLoop(*loopFile, *rosterFile)
	v.addTeam(*teamFile, *rosterFile)
	v.run()

	// Print errors first so they surface ahead of successes in merged
	// stdout/stderr (e.g. CI logs). In separate streams the ordering is
	// immaterial.
	for _, msg := range v.errors {
		fmt.Fprintf(os.Stderr, "error: %s\n", msg)
	}
	for _, msg := range v.oks {
		fmt.Fprintf(os.Stdout, "ok: %s\n", msg)
	}

	if len(v.errors) > 0 {
		return 1
	}
	return 0
}

type verifier struct {
	dir    string
	oks    []string
	errors []string
	seen   map[string]bool // visited config paths, to avoid re-validating or infinite recursion
}

func newVerifier(dir string) *verifier {
	return &verifier{
		dir:  dir,
		seen: map[string]bool{},
	}
}

func (v *verifier) addRoster(rosterPath, entryName string) {
	if rosterPath == "" {
		return
	}
	abs := v.resolve(rosterPath)
	if v.seen[abs] {
		return
	}
	v.seen[abs] = true

	roster, err := rosterconfig.Load(abs)
	if err != nil {
		v.errors = append(v.errors, fmt.Sprintf("roster %s: %v", rosterPath, err))
		return
	}
	v.oks = append(v.oks, fmt.Sprintf("roster %s (%d entries)", rosterPath, len(*roster)))

	if entryName != "" {
		if _, err := roster.Lookup(entryName); err != nil {
			v.errors = append(v.errors, fmt.Sprintf("roster %s: entry %q: %v", rosterPath, entryName, err))
		} else {
			v.oks = append(v.oks, fmt.Sprintf("roster %s: entry %q found", rosterPath, entryName))
		}
	}
}

func (v *verifier) addLoop(loopPath, rosterFallback string) {
	if loopPath == "" {
		return
	}
	abs := v.resolve(loopPath)
	if v.seen[abs] {
		return
	}
	v.seen[abs] = true

	fallback := v.resolve(rosterFallback)
	cfg, roster, err := looprunner.LoadLoopConfigWithRoster(abs, nil, fallback)
	if err != nil {
		v.errors = append(v.errors, fmt.Sprintf("loop %s: %v", loopPath, err))
		return
	}
	phaseCount := len(cfg.Pre) + len(cfg.Loop) + len(cfg.Post)
	v.oks = append(v.oks, fmt.Sprintf("loop %s (%d phases, max_iterations=%d)", loopPath, phaseCount, cfg.MaxIterations))

	configDir := filepath.Dir(abs)
	v.checkNested(roster, cfg.Pre, configDir)
	v.checkNested(roster, cfg.Loop, configDir)
	v.checkNested(roster, cfg.Post, configDir)
}

func (v *verifier) addTeam(teamPath, rosterFallback string) {
	if teamPath == "" {
		return
	}
	abs := v.resolve(teamPath)
	if v.seen[abs] {
		return
	}
	v.seen[abs] = true

	fallback := v.resolve(rosterFallback)
	cfg, roster, err := teamrunner.LoadTeamConfigWithRoster(abs, nil, fallback)
	if err != nil {
		v.errors = append(v.errors, fmt.Sprintf("team %s: %v", teamPath, err))
		return
	}
	phaseCount := len(cfg.Pre) + len(cfg.Team) + len(cfg.Post)
	v.oks = append(v.oks, fmt.Sprintf("team %s (%d phases)", teamPath, phaseCount))

	configDir := filepath.Dir(abs)
	v.checkNested(roster, cfg.Pre, configDir)
	v.checkNested(roster, cfg.Team, configDir)
	v.checkNested(roster, cfg.Post, configDir)
}

// checkNested recursively validates loop_file and team_file references in
// phases. These are only loaded on demand during a real run, so verify walks
// them eagerly to catch errors before a run starts.
func (v *verifier) checkNested(roster *rosterconfig.Config, phases []phaseconfig.Phase, configDir string) {
	for _, phase := range phases {
		if phase.LoopFile != "" {
			nestedPath := phase.LoopFile
			if !filepath.IsAbs(nestedPath) {
				nestedPath = filepath.Join(configDir, nestedPath)
			}
			v.addLoop(nestedPath, "")
		}
		if phase.TeamFile != "" {
			nestedPath := phase.TeamFile
			if !filepath.IsAbs(nestedPath) {
				nestedPath = filepath.Join(configDir, nestedPath)
			}
			v.addTeam(nestedPath, "")
		}
	}
}

func (v *verifier) resolve(path string) string {
	if path == "" {
		return ""
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(v.dir, path))
}

func (v *verifier) run() {
	// All work is done in the add* methods; this is a hook for future
	// cross-config consistency checks.
}
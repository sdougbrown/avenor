package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func testShellTool() Tool {
	return NewShellToolWithConfig(&ShellConfig{
		AllowedCommands: []string{"printf", "rg"},
		TimeoutSeconds:  5,
		MaxOutputBytes:  1024,
	})
}

func executeShell(t *testing.T, payload map[string]any) (string, error) {
	t.Helper()
	args, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return testShellTool().Execute(context.Background(), t.TempDir(), args)
}

func TestShellToolExecutesExplicitArgvLiterally(t *testing.T) {
	got, err := executeShell(t, map[string]any{
		"cmd":  "printf",
		"args": []string{"%s", `from ['\"]solid-js`},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != `from ['\"]solid-js` {
		t.Fatalf("output = %q, want exact argv argument", got)
	}
}

func TestShellToolExplicitArgvAllowsOperatorLiterals(t *testing.T) {
	got, err := executeShell(t, map[string]any{
		"cmd":  "printf",
		"args": []string{"%s", "|"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "|" {
		t.Fatalf("output = %q, want literal pipe", got)
	}
}

func TestShellToolSchemaExposesArgvOnly(t *testing.T) {
	schema := string(testShellTool().Schema())
	if strings.Contains(schema, `"command"`) {
		t.Fatalf("schema unexpectedly exposes legacy command field: %s", schema)
	}
	if !strings.Contains(schema, `"cmd"`) || !strings.Contains(schema, `"args"`) {
		t.Fatalf("schema missing argv fields: %s", schema)
	}
	if !strings.Contains(schema, `"required": ["cmd", "args"]`) {
		t.Fatalf("schema should require cmd and args: %s", schema)
	}
}

func TestShellToolLegacyCommandStillHonorsQuoteGrouping(t *testing.T) {
	got, err := executeShell(t, map[string]any{
		"command": `printf %s "hello world"`,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "hello world" {
		t.Fatalf("output = %q, want quoted argument preserved as one word", got)
	}
}

func TestShellToolLegacyCommandRejectsShellOperators(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"command": "printf hi | wc -c",
	})
	if err == nil {
		t.Fatal("expected shell operator rejection")
	}
	if !strings.Contains(err.Error(), "unsupported shell syntax") || !strings.Contains(err.Error(), "cmd/args") {
		t.Fatalf("error = %q, want actionable unsupported syntax message", err.Error())
	}
}

func TestShellToolLegacyCommandRejectsOperatorsWithoutSpaces(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"command": "printf hi|wc -c",
	})
	if err == nil {
		t.Fatal("expected shell operator rejection")
	}
	if !strings.Contains(err.Error(), "unsupported shell syntax") {
		t.Fatalf("error = %q, want unsupported syntax message", err.Error())
	}
}

func TestShellToolRejectsAmbiguousInputShape(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"cmd":     "printf",
		"command": "printf hi",
	})
	if err == nil {
		t.Fatal("expected ambiguous input rejection")
	}
	if !strings.Contains(err.Error(), "either cmd/args or command") {
		t.Fatalf("error = %q, want input shape guidance", err.Error())
	}
}

func TestShellToolRejectsCmdContainingSpacesWithRewriteHint(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"cmd":  "git diff",
		"args": []string{"--stat", "base...head"},
	})
	if err == nil {
		t.Fatal("expected cmd shape rejection")
	}
	if !strings.Contains(err.Error(), `cmd must be a bare executable`) || !strings.Contains(err.Error(), `"cmd":"git","args":["diff","--stat","base...head"]`) {
		t.Fatalf("error = %q, want rewrite hint", err.Error())
	}
}

func TestShellToolRejectsDuplicateExecutableInArgs(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"cmd":  "rg",
		"args": []string{"rg", "needle"},
	})
	if err == nil {
		t.Fatal("expected duplicate executable rejection")
	}
	if !strings.Contains(err.Error(), `args should not repeat cmd`) || !strings.Contains(err.Error(), `"cmd":"rg","args":["needle"]`) {
		t.Fatalf("error = %q, want duplicate-argv guidance", err.Error())
	}
}

func TestShellToolRejectsGitFlagsWithoutSubcommand(t *testing.T) {
	_, err := executeShell(t, map[string]any{
		"cmd":  "git",
		"args": []string{"--stat", "base...head"},
	})
	if err == nil {
		t.Fatal("expected git subcommand rejection")
	}
	if !strings.Contains(err.Error(), `git requires a subcommand before flags`) || !strings.Contains(err.Error(), `"cmd":"git","args":["diff","--stat","base...head"]`) {
		t.Fatalf("error = %q, want git subcommand hint", err.Error())
	}
}

func TestParseShellWordsGroupsRegexPattern(t *testing.T) {
	words, err := parseShellWords(`rg -l "from ['\"]solid-js" --type ts`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(words) != 5 {
		t.Fatalf("got %d words: %#v", len(words), words)
	}
	if words[2].Text != `from ['"]solid-js` || !words[2].Quoted {
		t.Fatalf("pattern word = %#v, want quoted regex as one argument", words[2])
	}
}

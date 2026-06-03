package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// cappedWriter is an io.Writer that stops writing after limit bytes.
type cappedWriter struct {
	w       io.Writer
	limit   int
	written int
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	remaining := c.limit - c.written
	if remaining <= 0 {
		return len(p), nil // silently discard
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := c.w.Write(p)
	c.written += n
	return len(p), err // report full length to avoid short-write errors upstream
}

// ShellConfig allows overriding shell tool defaults per-profile.
type ShellConfig struct {
	AllowedCommands []string `json:"allowed_commands,omitempty"`
	TimeoutSeconds  int      `json:"timeout_seconds,omitempty"`
	MaxOutputBytes  int      `json:"max_output_bytes,omitempty"`
}

// DefaultShellConfig returns a ShellConfig with built-in defaults.
func DefaultShellConfig() ShellConfig {
	return ShellConfig{
		AllowedCommands: allowedShellCommands,
		TimeoutSeconds:  30,
		MaxOutputBytes:  256 << 10,
	}
}

var allowedShellCommands = []string{
	"go", "git", "make", "mise", "npm", "bun", "node",
	"ls", "cat", "echo", "grep", "rg", "find", "head", "tail", "wc",
	"sort", "uniq", "diff", "mkdir", "rmdir", "cp", "mv", "rm", "printf",
	"chmod", "date", "pwd", "which", "test", "python", "python3",
	"sed", "awk", "jq", "tr", "cut", "xargs",
	"touch", "stat", "tee", "timeout", "bc", "expr", "tar", "zip", "unzip",
}

func NewShellTool() Tool {
	return NewShellToolWithConfig(nil)
}

// NewShellToolWithConfig creates a ShellTool with the given config overrides.
// Nil fields fall back to DefaultShellConfig values. Partial configs are merged
// with defaults so that unset fields keep sensible values.
func NewShellToolWithConfig(cfg *ShellConfig) Tool {
	t := &ShellTool{cfg: &ShellConfig{}}
	if cfg == nil {
		d := DefaultShellConfig()
		t.cfg = &d
		return t
	}
	// Merge user config with defaults
	d := DefaultShellConfig()
	if cfg.AllowedCommands != nil {
		t.cfg.AllowedCommands = cfg.AllowedCommands
	} else {
		t.cfg.AllowedCommands = d.AllowedCommands
	}
	if cfg.TimeoutSeconds > 0 {
		t.cfg.TimeoutSeconds = cfg.TimeoutSeconds
	} else {
		t.cfg.TimeoutSeconds = d.TimeoutSeconds
	}
	if cfg.MaxOutputBytes > 0 {
		t.cfg.MaxOutputBytes = cfg.MaxOutputBytes
	} else {
		t.cfg.MaxOutputBytes = d.MaxOutputBytes
	}
	return t
}

type ShellTool struct {
	cfg *ShellConfig
}

type shellInput struct {
	Command string   `json:"command,omitempty"`
	Cmd     string   `json:"cmd,omitempty"`
	Args    []string `json:"args,omitempty"`
}

func (t *ShellTool) Name() string { return "shell" }

func (t *ShellTool) Description() string {
	return fmt.Sprintf("Run a command and return its output. Prefer cmd plus args for exact argv execution. cmd must be only the executable name; put subcommands and flags in args. Examples: {\"cmd\":\"git\",\"args\":[\"diff\",\"--stat\",\"base...head\"]}, {\"cmd\":\"git\",\"args\":[\"diff\",\"--name-only\",\"base...head\"]}, and {\"cmd\":\"ls\",\"args\":[\"packages\"]}. Do not use {\"cmd\":\"git diff\",...}, {\"cmd\":\"git\",\"args\":[\"--stat\",...]}, or repeat the executable in args. The legacy command string supports quote grouping but no shell interpreter, so pipes, redirects, shell builtins, command substitution, and command chaining do not work. Timeout: %ds, output cap: %dKB.",
		t.cfg.TimeoutSeconds, t.cfg.MaxOutputBytes/1024)
}

func (t *ShellTool) Schema() json.RawMessage {
	return json.RawMessage(`{
		"type": "object",
		"properties": {
			"cmd": {
				"type": "string",
				"description": "Executable to run directly, without spaces or flags (e.g. git, rg, ls)"
			},
			"args": {
				"type": "array",
				"items": {"type": "string"},
				"description": "Arguments passed exactly as argv to cmd, including subcommands and flags. Example: [\"diff\",\"--stat\",\"base...head\"]"
			}
		},
		"required": ["cmd", "args"]
	}`)
}

func (t *ShellTool) Execute(ctx context.Context, workingDir string, args json.RawMessage) (string, error) {
	var input shellInput
	if err := json.Unmarshal(args, &input); err != nil {
		return "", fmt.Errorf("shell: invalid args: %w", err)
	}
	parts, err := input.argv()
	if err != nil {
		return "", err
	}
	if !t.isCommandAllowed(parts[0]) {
		return "", fmt.Errorf("shell: command %q is not in the allowed list", parts[0])
	}

	timeout := time.Duration(t.cfg.TimeoutSeconds) * time.Second
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, parts[0], parts[1:]...)
	cmd.Dir = workingDir

	// Cap output to prevent runaway output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &cappedWriter{w: &stdout, limit: t.cfg.MaxOutputBytes}
	cmd.Stderr = &cappedWriter{w: &stderr, limit: t.cfg.MaxOutputBytes}

	if err := cmd.Run(); err != nil {
		output := strings.TrimSpace(stdout.String())
		errOutput := strings.TrimSpace(stderr.String())
		var parts []string
		if output != "" {
			parts = append(parts, output)
		}
		if errOutput != "" {
			parts = append(parts, errOutput)
		}
		parts = append(parts, fmt.Sprintf("error: %v", err))
		return strings.Join(parts, "\n"), nil
	}

	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("shell: %w", err)
	}

	return strings.TrimSpace(stdout.String()), nil
}

func (t *ShellTool) isCommandAllowed(cmd string) bool {
	for _, allowed := range t.cfg.AllowedCommands {
		if cmd == allowed {
			return true
		}
	}
	return false
}

func (input shellInput) argv() ([]string, error) {
	hasCommand := strings.TrimSpace(input.Command) != ""
	hasCmd := strings.TrimSpace(input.Cmd) != ""
	if hasCommand && hasCmd {
		return nil, fmt.Errorf("shell: provide either cmd/args or command, not both")
	}
	if hasCmd {
		if err := validateArgv(input.Cmd, input.Args); err != nil {
			return nil, err
		}
		return append([]string{input.Cmd}, input.Args...), nil
	}
	if !hasCommand {
		return nil, fmt.Errorf("shell: command is required")
	}

	words, err := parseShellWords(input.Command)
	if err != nil {
		return nil, fmt.Errorf("shell: %w", err)
	}
	if len(words) == 0 {
		return nil, fmt.Errorf("shell: command is required")
	}
	for _, word := range words {
		if err := validateLegacyWord(word); err != nil {
			return nil, err
		}
	}
	parts := make([]string, len(words))
	for i, word := range words {
		parts[i] = word.Text
	}
	return parts, nil
}

func validateArgv(cmd string, args []string) error {
	if strings.TrimSpace(cmd) == "" {
		return fmt.Errorf("shell: cmd is required")
	}
	if fields := strings.Fields(cmd); len(fields) > 1 {
		rewriteArgs := append(fields[1:], args...)
		return fmt.Errorf("shell: cmd must be a bare executable name, not a full command string. Rewrite as {\"cmd\":%q,\"args\":%s}", fields[0], quotedJSONArray(rewriteArgs))
	}
	if isShellOperator(cmd) || looksLikeRedirection(cmd) || containsShellMetachar(cmd) ||
		strings.Contains(cmd, "$(") || strings.Contains(cmd, "`") {
		return unsupportedShellSyntaxError(cmd)
	}
	if len(args) > 0 && args[0] == cmd {
		return fmt.Errorf("shell: args should not repeat cmd. Rewrite as {\"cmd\":%q,\"args\":%s}", cmd, quotedJSONArray(args[1:]))
	}
	if cmd == "git" && len(args) > 0 && strings.HasPrefix(args[0], "-") {
		return fmt.Errorf("shell: git requires a subcommand before flags. Rewrite as {\"cmd\":\"git\",\"args\":[\"diff\",%s]}", strings.TrimPrefix(quotedJSONArray(args), "["))
	}
	return nil
}

func quotedJSONArray(items []string) string {
	data, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(data)
}

func validateLegacyWord(word shellWord) error {
	if word.Quoted {
		return nil
	}
	if isShellOperator(word.Text) || looksLikeRedirection(word.Text) || containsShellMetachar(word.Text) ||
		strings.Contains(word.Text, "$(") || strings.Contains(word.Text, "`") {
		return unsupportedShellSyntaxError(word.Text)
	}
	return nil
}

func unsupportedShellSyntaxError(token string) error {
	return fmt.Errorf("shell: unsupported shell syntax %q. Pony shell executes commands directly and does not support shell operators like |, >, <, &&, ||, $(), or backticks. Use separate tool calls or pass argv via cmd/args", token)
}

func isShellOperator(token string) bool {
	switch token {
	case "|", ">", "<", ">>", "<<", "&&", "||", ";":
		return true
	default:
		return false
	}
}

func containsShellMetachar(token string) bool {
	return strings.ContainsAny(token, "|<>;") || strings.Contains(token, "&&") || strings.Contains(token, "||")
}

func looksLikeRedirection(token string) bool {
	if token == "" {
		return false
	}
	i := 0
	for i < len(token) && token[i] >= '0' && token[i] <= '9' {
		i++
	}
	if i >= len(token) {
		return false
	}
	return token[i] == '>' || token[i] == '<'
}

type shellWord struct {
	Text   string
	Quoted bool
}

func parseShellWords(command string) ([]shellWord, error) {
	var words []shellWord
	var b strings.Builder
	var quote byte
	inWord := false
	quoted := false

	flush := func() {
		if !inWord {
			return
		}
		words = append(words, shellWord{Text: b.String(), Quoted: quoted})
		b.Reset()
		inWord = false
		quoted = false
	}

	for i := 0; i < len(command); i++ {
		c := command[i]
		if quote == 0 && (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
			flush()
			continue
		}
		inWord = true
		switch {
		case quote == 0 && (c == '\'' || c == '"'):
			quote = c
			quoted = true
		case quote != 0 && c == quote:
			quote = 0
		case c == '\\' && quote != '\'':
			if i+1 >= len(command) {
				b.WriteByte(c)
				continue
			}
			i++
			b.WriteByte(command[i])
		default:
			b.WriteByte(c)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote in command")
	}
	flush()
	return words, nil
}

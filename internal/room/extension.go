package room

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ArbiterExtensionSource is installed into <dir>/.pi/extensions/ so every pi
// head working in the shared workspace routes file-mutating tool calls through
// the permission relay, where the room's arbiter resolves them. The denial
// reason is carried in the block result because the workspace is shared and
// stale writes are the failure mode being guarded against.
const ArbiterExtensionSource = "// Installed by the room coordinator: gate file-mutating tools so the room\n" +
	"// arbiter (the control client) can serialize mutations during parallel work.\n" +
	"const GATED = new Set([\"write\", \"edit\", \"multiedit\", \"notebookedit\", \"apply_patch\"]);\n\n" +
	"export default function roomArbiter(pi) {\n" +
	"\tpi.on(\"tool_call\", async (event, ctx) => {\n" +
	"\t\tif (!GATED.has(event.toolName)) return undefined;\n" +
	"\t\tconst ok = await ctx.ui.confirm(\n" +
	"\t\t\t\"Room arbiter\",\n" +
	"\t\t\t\"Approve this workspace mutation?\",\n" +
	"\t\t);\n" +
	"\t\tif (ok) return undefined;\n" +
	"\t\treturn {\n" +
	"\t\t\tblock: true,\n" +
	"\t\t\treason:\n" +
	"\t\t\t\t\"Room arbiter denied this write: another head may have mutated the shared workspace since your last observation. Re-read the affected files, reconcile your change with theirs, and retry.\",\n" +
	"\t\t};\n" +
	"\t});\n" +
	"}\n"

// InstallArbiterExtension writes the gate extension into the workspace's
// project extension directory. Idempotent.
func InstallArbiterExtension(dir string) (string, error) {
	path := filepath.Join(dir, ".pi", "extensions", "room-arbiter.ts")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(ArbiterExtensionSource), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// EnsurePiTrust grants pi project trust for the workspace so heads load the
// room's arbiter extension (RPC mode cannot show the built-in trust prompt,
// and the default "ask" policy skips project extensions entirely). The grant
// is for the room's own workspace and extension; callers surface it in the
// room log so the operator can see it happened.
func EnsurePiTrust(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	path := filepath.Join(home, ".pi", "agent", "trust.json")
	data := map[string]any{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &data); err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
	}
	if v, ok := data[abs].(bool); ok && v {
		return nil
	}
	data[abs] = true
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

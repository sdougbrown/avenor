package supervisor

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestConversationSocketDerivesTheDocumentedPath pins the derivation a
// conversation's supervisor socket follows. The feed that binds a conversation
// and the commands that read its binding compute this path independently, so a
// change to it silently splits a session across two supervisors. The vector is an
// internal implementation pin, not an interface: nothing outside this package
// should compute the name, and the derived path is never a flag, an env value or
// a tool argument.
func TestConversationSocketDerivesTheDocumentedPath(t *testing.T) {
	t.Setenv("AVENOR_HOME", "/scratch/avenor")
	got, err := ConversationSocket("poo", "ses_example")
	if err != nil {
		t.Fatalf("ConversationSocket: %v", err)
	}
	want := filepath.Join("/scratch/avenor", "sockets", "avenor-conv-d64c3472666b1df07e73.sock")
	if got != want {
		t.Fatalf("ConversationSocket(poo, ses_example) = %q, want %q", got, want)
	}
}

// TestConversationSocketSeparatesConversations pins what makes two sessions
// distinct: the native host and the session id are both part of the derivation,
// and neither collides with the other's pair.
func TestConversationSocketSeparatesConversations(t *testing.T) {
	t.Setenv("AVENOR_HOME", "/scratch/avenor")
	seen := map[string]string{}
	for _, pair := range []struct{ host, session string }{
		{"poo", "ses_example"},
		{"poo", "ses_other"},
		{"opencode", "ses_example"},
		{"opencode", "ses_other"},
	} {
		path, err := ConversationSocket(pair.host, pair.session)
		if err != nil {
			t.Fatalf("ConversationSocket(%q, %q): %v", pair.host, pair.session, err)
		}
		key := pair.host + "\x00" + pair.session
		if other, ok := seen[path]; ok {
			t.Fatalf("%q and %q derive the same socket %q", key, other, path)
		}
		seen[path] = key
	}
}

// TestConversationSocketStaysInsideTheSocketsRoot pins the shape a derived
// socket has to keep: a bounded single-file name in the sockets root, so a
// 200-byte session id still yields a path under the unix socketname limit.
func TestConversationSocketStaysInsideTheSocketsRoot(t *testing.T) {
	t.Setenv("AVENOR_HOME", "/scratch/avenor")
	session := strings.Repeat("s", 200)
	path, err := ConversationSocket("poo", session)
	if err != nil {
		t.Fatalf("ConversationSocket: %v", err)
	}
	if dir := filepath.Dir(path); dir != filepath.Join("/scratch/avenor", "sockets") {
		t.Fatalf("socket directory = %q, want the sockets root", dir)
	}
	name := filepath.Base(path)
	if !strings.HasPrefix(name, "avenor-conv-") || !strings.HasSuffix(name, ".sock") {
		t.Fatalf("socket name = %q, want the conversation socket naming", name)
	}
	if len(name) > 64 {
		t.Fatalf("socket name = %q (%d bytes), want a bounded name", name, len(name))
	}
}

// TestConversationSocketFallsBackToTheHomeRoot pins the default root when
// AVENOR_HOME names nothing: the same ~/.avenor/sockets the MCP supervisor socket
// and the documented supervisor_id rule use.
func TestConversationSocketFallsBackToTheHomeRoot(t *testing.T) {
	home := t.TempDir()
	t.Setenv("AVENOR_HOME", "")
	t.Setenv("HOME", home)
	path, err := ConversationSocket("poo", "ses_example")
	if err != nil {
		t.Fatalf("ConversationSocket: %v", err)
	}
	if got, want := filepath.Dir(path), filepath.Join(home, ".avenor", "sockets"); got != want {
		t.Fatalf("socket directory = %q, want %q", got, want)
	}
}

// TestConversationSocketRefusesAnIncompletePair pins that a socket is only
// derived for a real conversation: half a pair names no session to serve.
func TestConversationSocketRefusesAnIncompletePair(t *testing.T) {
	for _, pair := range []struct{ host, session string }{{"", "ses"}, {"poo", ""}, {"", ""}} {
		if _, err := ConversationSocket(pair.host, pair.session); err == nil {
			t.Fatalf("ConversationSocket(%q, %q) = nil error, want a refusal", pair.host, pair.session)
		}
	}
}

// TestSocketDirHonoursAvenorHome pins the one root rule the derived sockets and
// the default supervisor socket share.
func TestSocketDirHonoursAvenorHome(t *testing.T) {
	t.Setenv("AVENOR_HOME", "/scratch/avenor")
	if got, want := mustSocketDir(t), filepath.Join("/scratch/avenor", "sockets"); got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
	t.Setenv("AVENOR_HOME", "")
	t.Setenv("HOME", "/scratch/home")
	if got, want := mustSocketDir(t), filepath.Join("/scratch/home", ".avenor", "sockets"); got != want {
		t.Fatalf("SocketDir() = %q, want %q", got, want)
	}
}

func mustSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := SocketDir()
	if err != nil {
		t.Fatalf("SocketDir: %v", err)
	}
	return dir
}

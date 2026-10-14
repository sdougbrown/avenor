package launcher

import (
	"context"
	"os/exec"
	"slices"
	"testing"
)

func TestArgvWithoutWrapper(t *testing.T) {
	got := Argv("agy", []string{"--print", "hi"}, nil)
	want := []string{"agy", "--print", "hi"}
	if !slices.Equal(got, want) {
		t.Fatalf("Argv() = %#v, want %#v", got, want)
	}
}

func TestArgvWithWrapper(t *testing.T) {
	got := Argv("agy", []string{"--print", "hi"}, []string{"nice", "-n", "5"})
	want := []string{"nice", "-n", "5", "agy", "--print", "hi"}
	if !slices.Equal(got, want) {
		t.Fatalf("Argv() = %#v, want %#v", got, want)
	}
}

// TestCommandWithoutWrapperMatchesExecCommand pins the no-wrapper behavior:
// the built command must be byte-for-byte what exec.CommandContext produces.
func TestCommandWithoutWrapperMatchesExecCommand(t *testing.T) {
	got := Command(context.Background(), nil, "agy", "--print", "hi")
	want := exec.CommandContext(context.Background(), "agy", "--print", "hi")
	if got.Path != want.Path {
		t.Fatalf("Path = %q, want %q", got.Path, want.Path)
	}
	if !slices.Equal(got.Args, want.Args) {
		t.Fatalf("Args = %#v, want %#v", got.Args, want.Args)
	}
	if got.Err != nil && want.Err == nil || got.Err == nil && want.Err != nil {
		t.Fatalf("Err = %v, want %v", got.Err, want.Err)
	}
	if got.Err != nil && got.Err.Error() != want.Err.Error() {
		t.Fatalf("Err = %v, want %v", got.Err, want.Err)
	}
}

func TestCommandPrependsWrapper(t *testing.T) {
	cmd := Command(context.Background(), []string{"nice", "-n", "5"}, "agy", "--print", "hi")
	if !slices.Equal(cmd.Args, []string{"nice", "-n", "5", "agy", "--print", "hi"}) {
		t.Fatalf("Args = %#v, want wrapper-prefixed argv", cmd.Args)
	}
}

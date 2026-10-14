package mcpserver

import (
	"context"
	"reflect"
	"testing"
)

func TestAvenorSpawnThreadsLaunchWrapper(t *testing.T) {
	fake := &fakeClient{
		spawnResult: map[string]any{
			"runtime_id": "rt_wrap_1",
			"session_id": "ses_wrap_1",
		},
	}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:       "/tmp/test-repo",
		LaunchWrapper: []string{"nice", "-n", "5"},
	})
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	p := fake.spawnCapturedParams
	if p == nil {
		t.Fatal("expected spawn params to be captured")
	}
	wrapper, ok := p["launch_wrapper"].([]string)
	if !ok {
		t.Fatalf("params[launch_wrapper] = %#v, want []string", p["launch_wrapper"])
	}
	if !reflect.DeepEqual(wrapper, []string{"nice", "-n", "5"}) {
		t.Fatalf("params[launch_wrapper] = %#v, want [nice -n 5]", wrapper)
	}
}

func TestAvenorSpawnRejectsEmptyLaunchWrapperElement(t *testing.T) {
	fake := &fakeClient{}
	s, err := NewServer(Options{Transport: "stdio", NoAutostart: true, ControlClient: fake})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.handleAvenorSpawn(context.Background(), nil, spawnArgs{
		RepoDir:       "/tmp/test-repo",
		LaunchWrapper: []string{"nice", ""},
	})
	if err == nil {
		t.Fatal("spawn with an empty launch wrapper element succeeded, want error")
	}
	if err.Error() != "launch_wrapper: launch wrapper elements must be non-empty" {
		t.Fatalf("spawn error = %q, want %q", err, "launch_wrapper: launch wrapper elements must be non-empty")
	}
}

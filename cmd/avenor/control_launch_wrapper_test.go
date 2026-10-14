package main

import (
	"bytes"
	"slices"
	"testing"
)

func TestSpawnParamsFromArgsLaunchWrapperRepeatable(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{
		"--prompt", "work",
		"--launch-wrapper", "nice",
		"--launch-wrapper", "-n",
		"--launch-wrapper", "5",
	}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	got, ok := params["launch_wrapper"].([]string)
	if !ok {
		t.Fatalf("params[launch_wrapper] = %#v, want []string", params["launch_wrapper"])
	}
	if !slices.Equal(got, []string{"nice", "-n", "5"}) {
		t.Fatalf("launch_wrapper = %#v, want [nice -n 5]", got)
	}
}

func TestSpawnParamsFromArgsOmitsLaunchWrapperByDefault(t *testing.T) {
	var stderr bytes.Buffer
	params, code := spawnParamsFromArgs([]string{"--prompt", "work"}, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr=%s", code, stderr.String())
	}
	if _, ok := params["launch_wrapper"]; ok {
		t.Fatalf("launch_wrapper = %#v, want absent", params["launch_wrapper"])
	}
}

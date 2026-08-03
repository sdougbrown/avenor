// Package thinkingpolicy validates the canonical thinking-value contract from a
// portable Umpire schema shared with the TypeScript packages.
package thinkingpolicy

//go:generate go run github.com/umpire-tools/umpire-go-gen@v0.1.1 -i ../../schemas/thinking_policy.umpire.json -output-file thinking_policy.gen.go -pkg thinkingpolicy

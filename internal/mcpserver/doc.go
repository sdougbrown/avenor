// Package mcpserver is the Go MCP surface: the tools a client calls, the
// supervisor client they share, and the schema those tools publish.
//
// schemas/spawn.input.json is the single source for the spawn tool's input:
// every field's name, type, and description, plus which surfaces publish it.
// The server cannot read a repository-relative file at run time, so
// `go generate ./internal/mcpserver` compiles it into spawn_schema.gen.go and
// the tool publishes that copy verbatim. Edit the schema file, not the
// generated artifact, and not a description string in this package.
//
// The JavaScript surfaces read the same file directly: packages/core imports
// it and every JS tool builds its field text from spawnFieldDescriptions.
// TestSpawnSchemaMatchesStruct holds this package's spawnArgs to the schema, so
// a field added to one without the other fails here rather than at a client.
package mcpserver

//go:generate go run ./cmd/genschema -input ../../schemas/spawn.input.json -out spawn_schema.gen.go -pkg mcpserver

// Package workflowtool implements the merged avenor_workflow tool: input
// admission, the per-action dispatch, and the typed tool errors.
//
//go:generate go run ./cmd/genschema -input ../../schemas/workflow.input.json -flat-out ../../schemas/workflow.input.flat.json -conformance ../../schemas/workflow_route.conformance.json -out schema.gen.go
package workflowtool

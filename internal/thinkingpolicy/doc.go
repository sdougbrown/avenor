// Package thinkingpolicy validates the canonical thinking-value contract and
// the per-backend thinking policy from a portable Umpire schema shared with the
// TypeScript packages.
//
// The Umpire schema (schemas/thinking_policy.umpire.json) is the single file of
// truth for both canonical value validity and backend-specific thinking support.
// It uses:
//
//   - A check rule for canonical value validation (off, minimal, low, medium,
//     high, xhigh, max).
//   - Conditions (backend, resume) and an eitherOf group with fairWhen branches
//     that encode which canonical values each backend supports on a fresh start
//     versus an explicit resume.
//
// The generated Check function evaluates both the canonical check and the
// backend policy branches. The hand-written Evaluate function wraps the
// generated Check to produce a four-valued Outcome (OK, UnsupportedCapability,
// UnsupportedValue, StartOnly) by inspecting the generated Fair flag and, when
// needed, re-evaluating with resume=false to distinguish StartOnly from
// UnsupportedValue.
//
// The set of backends with thinking support is derived from the schema's
// eitherOf branches at init time, so adding a new backend with thinking support
// requires only a schema change — no hand-written policy table updates.
//
// ValidateThinkingForBackendResume is exported as part of the portable contract
// for callers that need to validate resume-time thinking independently. The
// current follow-up path does not accept a thinking value (thinking is set at
// session start), so this function has no production callers yet; it is
// available for hosts and future wiring.
package thinkingpolicy

//go:generate go run github.com/umpire-tools/umpire-go-gen@v0.1.1 -i ../../schemas/thinking_policy.umpire.json -output-file thinking_policy.gen.go -pkg thinkingpolicy

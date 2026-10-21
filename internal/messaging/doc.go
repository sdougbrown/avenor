// Package messaging is the shared messaging surface: the transport interface
// every Go surface speaks, the typed outcome vocabulary, the message-id and
// generation primitives, and the merged messaging tools' contract.
//
// schemas/ in this repository is the single source of truth for those tools.
// Two generators compile it into the checked-in Go artifacts, both driven by
// `go generate ./internal/messaging/...`:
//
//   - umpire-go-gen compiles schemas/message_route.umpire.json into
//     message_route.gen.go: MessageRouteFields, MessageRouteConditions and
//     Check, which is the route-precondition table plus the reason each rule
//     denies with. The router reads those reasons for the two permission rules
//     and for an invalid action; every other row's text is a template from the
//     conformance fixture. Each field has one slot per rule kind, so no two
//     rules share a slot: the action enum is the check rule on action;
//     control_reply support is the enabledWhen slot on target and the resume
//     caller rule is its fairWhen slot; the two never-user-visible state guards
//     (tell/ask want a live target, resume wants a resumable one) are the
//     enabledWhen and fairWhen slots on message. Edit the .umpire.json file for
//     a rule about who may call an action, what state the target must be in,
//     and the sentence a denial carries.
//   - cmd/genschema compiles schemas/message.input.json,
//     schemas/peers.input.json, schemas/message_route.conformance.json and
//     schemas/message.outcome.json into schema.gen.go: the two input JSON
//     Schemas as string constants, the per-action applicability tables
//     (permitted keys, required keys and declared types from the conformance
//     fixture, declared length, pattern and numeric limits from the input
//     schemas), the repair templates, the outcome matrix with its render
//     templates, and the peers `last` templates. Edit these files for a rule
//     about an action's input or the text a result or failure carries: which
//     properties an action admits, a property's presence, type or constraint, a
//     repair sentence, an outcome's flags or rendered text. The conformance
//     fixture is also read at run time by the two case loaders, so a test can
//     hold the checked-in file and the compiled artifact against each other. The
//     per-action union is the contract source; every MCP surface publishes the
//     flat object schema compiled from it, because a `oneOf` union loses the
//     integer and object types in at least one real client.
//
// Nothing in this package imports a surface package: the router lives in
// internal/messaging/router so it may import this package and askclient
// together.
package messaging

//go:generate go run github.com/umpire-tools/umpire-go-gen@v0.1.1 -i ../../schemas/message_route.umpire.json -output-file message_route.gen.go -pkg messaging
//go:generate go run ./cmd/genschema -message-input ../../schemas/message.input.json -peers-input ../../schemas/peers.input.json -message-flat-out ../../schemas/message.input.flat.json -peers-flat-out ../../schemas/peers.input.flat.json -conformance ../../schemas/message_route.conformance.json -outcome ../../schemas/message.outcome.json -out schema.gen.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/sdougbrown/avenor/client"
	clicontract "github.com/sdougbrown/cli-contract"
)

// workflowClient dials the control socket named by the persistent --socket
// flag.
func workflowClient(inv clicontract.Invocation) (*client.Client, error) {
	socket := inv.Flags["socket"]
	if socket == "" {
		return nil, usageErrorf("avenor workflow: --socket is required")
	}
	return client.Dial(socket)
}

// workflowRequestFile reads a request file and decodes it as a strict JSON
// object.
func workflowRequestFile(name, path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: %s: %w", name, err)
	}
	var req map[string]any
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, usageErrorf("avenor workflow: %s: request file must be a JSON object: %v", name, err)
	}
	return req, nil
}

// workflowRequireKeys verifies that the request object carries each key.
func workflowRequireKeys(name string, req map[string]any, keys []string) error {
	for _, k := range keys {
		if _, ok := req[k]; !ok {
			return usageErrorf("avenor workflow: %s: request file is missing %q", name, k)
		}
	}
	return nil
}

// handleWorkflowCreate registers a workflow template from a JSON file.
func handleWorkflowCreate(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	requestFile := inv.Flags["request-file"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	data, err := os.ReadFile(requestFile)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: create: %w", err)
	}
	result, err := c.WorkflowCreate(json.RawMessage(data))
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: create: %w", err)
	}
	return result, nil
}

// handleWorkflowInstantiate starts one workflow instance from a template.
func handleWorkflowInstantiate(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	templateID := inv.Flags["template-id"]
	templateVersion := inv.Flags["template-version"]
	requestFile := inv.Flags["request-file"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	instanceData, err := os.ReadFile(requestFile)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: instantiate: %w", err)
	}
	var instance struct {
		Metadata map[string]any    `json:"metadata,omitempty"`
		Params   map[string]string `json:"params,omitempty"`
	}
	if err := json.Unmarshal(instanceData, &instance); err != nil {
		return nil, usageErrorf("avenor workflow: instantiate: request file must be a JSON object: %v", err)
	}
	var metadata map[string]any
	if len(instance.Metadata) > 0 {
		metadata = instance.Metadata
	}
	var params map[string]string
	if len(instance.Params) > 0 {
		params = instance.Params
	}
	result, err := c.WorkflowInstantiate(templateID, templateVersion, metadata, params)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: instantiate: %w", err)
	}
	return result, nil
}

// handleWorkflowStatus prints one workflow instance's status.
func handleWorkflowStatus(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowStatus(inv.Args[0])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: status: %w", err)
	}
	return result, nil
}

// handleWorkflowWait blocks until one workflow instance settles.
func handleWorkflowWait(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	if timeout := inv.Durations["timeout"]; timeout < 0 {
		return nil, usageErrorf("avenor workflow: wait: --timeout must not be negative")
	}
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowWait(inv.Args[0], inv.Durations["timeout"])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: wait: %w", err)
	}
	return result, nil
}

// handleWorkflowInspect prints one workflow instance's full record.
func handleWorkflowInspect(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowInspect(inv.Args[0])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: inspect: %w", err)
	}
	return result, nil
}

// handleWorkflowEvents lists one workflow instance's events.
func handleWorkflowEvents(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowEvents(inv.Args[0], int64(inv.Ints["after-seq"]), inv.Ints["limit"])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: events: %w", err)
	}
	return result, nil
}

// handleWorkflowComplete completes a machine/external handoff activation.
// The request file carries owner_token, outcome, outputs, and artifacts (the
// manager's workflow.complete fields); the four scalar flags carry the rest.
func handleWorkflowComplete(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	activationID := inv.Flags["activation-id"]
	attemptID := inv.Flags["attempt-id"]
	leaseID := inv.Flags["lease-id"]
	requestFile := inv.Flags["request-file"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req, err := workflowRequestFile("complete", requestFile)
	if err != nil {
		return nil, err
	}
	if err := workflowRequireKeys("complete", req, []string{"owner_token", "outcome", "outputs", "artifacts"}); err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(req)+4)
	for k, v := range req {
		fields[k] = v
	}
	fields["node_id"] = inv.Args[1]
	fields["activation_id"] = activationID
	fields["attempt_id"] = attemptID
	fields["lease_id"] = leaseID
	result, err := c.WorkflowComplete(inv.Args[0], fields)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: complete: %w", err)
	}
	return result, nil
}

// handleWorkflowGate records a gate decision on a parked awaiting_gate
// activation. The operation is validated against the closed enum before any
// server call so an unknown value fails fast without mutating the store. The
// request file carries the operation's fields (actor, reason, outcome,
// subject, poll_id, source, result, response_hash, observed_at,
// evidence_ids).
func handleWorkflowGate(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	activationID := inv.Flags["activation-id"]
	operation := inv.Flags["operation"]
	requestFile := inv.Flags["request-file"]
	switch operation {
	case "satisfy", "reject", "waive", "external_result":
	default:
		return nil, usageErrorf("avenor workflow: gate: unknown operation %q (allowed: satisfy, reject, waive, external_result)", operation)
	}
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req, err := workflowRequestFile("gate", requestFile)
	if err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(req)+4)
	for k, v := range req {
		fields[k] = v
	}
	fields["node_id"] = inv.Args[1]
	fields["activation_id"] = activationID
	fields["operation"] = operation
	fields["gate_id"] = inv.Args[2]
	result, err := c.WorkflowGate(inv.Args[0], fields)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: gate: %w", err)
	}
	return result, nil
}

// handleWorkflowSkip waives every unsatisfied required gate on a parked
// awaiting_gate activation. The request file carries actor, reason,
// evidence_ids, and optionally activation_id.
func handleWorkflowSkip(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	requestFile := inv.Flags["request-file"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req, err := workflowRequestFile("skip", requestFile)
	if err != nil {
		return nil, err
	}
	if err := workflowRequireKeys("skip", req, []string{"actor", "reason", "evidence_ids"}); err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(req)+1)
	for k, v := range req {
		fields[k] = v
	}
	fields["node_id"] = inv.Args[1]
	result, err := c.WorkflowSkip(inv.Args[0], fields)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: skip: %w", err)
	}
	return result, nil
}

// handleWorkflowCancel cancels a whole workflow instance. --reason is
// required (the durable audit trail of the workflow.canceled event); --actor
// is optional and defaults to "cli". The control method also terminates the
// instance's in-flight attempt runtimes supervisor-side.
func handleWorkflowCancel(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	reason := inv.Flags["reason"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowCancel(inv.Args[0], orDefault(inv.Flags["actor"], "cli"), reason)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: cancel: %w", err)
	}
	return result, nil
}

// handleWorkflowUnblock returns a blocked activation to ready. The request
// file carries actor, reason, and optionally activation_id.
func handleWorkflowUnblock(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	requestFile := inv.Flags["request-file"]
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req, err := workflowRequestFile("unblock", requestFile)
	if err != nil {
		return nil, err
	}
	if err := workflowRequireKeys("unblock", req, []string{"actor", "reason"}); err != nil {
		return nil, err
	}
	fields := make(map[string]any, len(req)+1)
	for k, v := range req {
		fields[k] = v
	}
	fields["node_id"] = inv.Args[1]
	result, err := c.WorkflowUnblock(inv.Args[0], fields)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow: unblock: %w", err)
	}
	return result, nil
}

// handleWorkflowReady lists a controller's ready work items.
func handleWorkflowReady(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowReady(inv.Args[0], inv.Ints["limit"])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow ready: %w", err)
	}
	return result, nil
}

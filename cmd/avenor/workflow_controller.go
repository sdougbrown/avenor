package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	clicontract "github.com/sdougbrown/cli-contract"
)

// handleWorkflowControllerCreate registers a durable controller from a JSON
// file.
func handleWorkflowControllerCreate(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	requestFile := inv.Flags["request-file"]
	if requestFile == "" {
		return nil, usageErrorf("avenor workflow controller create: --request-file is required")
	}
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	data, err := os.ReadFile(requestFile)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller create: %w", err)
	}
	result, err := c.WorkflowControllerCreate(json.RawMessage(data))
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller create: %w", err)
	}
	return result, nil
}

// handleWorkflowControllerEnable enables one controller.
func handleWorkflowControllerEnable(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowControllerEnable(inv.Args[0])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller enable: %w", err)
	}
	return result, nil
}

// handleWorkflowControllerDisable disables one controller; --reason is the
// durable audit trail.
func handleWorkflowControllerDisable(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	reason := inv.Flags["reason"]
	if reason == "" {
		return nil, usageErrorf("avenor workflow controller disable: --reason is required")
	}
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowControllerDisable(inv.Args[0], reason)
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller disable: %w", err)
	}
	return result, nil
}

// handleWorkflowControllerStatus prints one controller's status.
func handleWorkflowControllerStatus(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowControllerStatus(inv.Args[0])
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller status: %w", err)
	}
	return result, nil
}

// handleWorkflowControllerList prints every controller on the socket.
func handleWorkflowControllerList(ctx context.Context, inv clicontract.Invocation, stderr io.Writer) (any, error) {
	c, err := workflowClient(inv)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	result, err := c.WorkflowControllerList()
	if err != nil {
		return nil, fmt.Errorf("avenor workflow controller list: %w", err)
	}
	return result, nil
}

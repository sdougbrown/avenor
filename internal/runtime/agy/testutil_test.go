package agy

import (
	"encoding/json"
	"io"
)

// writeJSONL writes a single JSON object as a JSONL line to the writer.
func writeJSONL(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// sendEvents writes JSONL events to the writer then closes it if closable.
func sendEvents(w io.Writer, events ...map[string]any) {
	sendEventsNoClose(w, events...)
	if cw, ok := w.(io.Closer); ok {
		_ = cw.Close()
	}
}

func sendEventsNoClose(w io.Writer, events ...map[string]any) {
	for _, evt := range events {
		_ = writeJSONL(w, evt)
	}
}

// test helpers for common agy JSONL event payloads.

func newInit(conversationID, model, agyVersion string) map[string]any {
	return map[string]any{
		"event":           "init",
		"conversation_id": conversationID,
		"init":            map[string]any{},
	}
}

func newAgentActive(stepIndex int, text string) map[string]any {
	return map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": stepIndex,
			"step_type":  "agent_response",
			"state":      "ACTIVE",
			"text_delta": text,
		},
	}
}

func newAgentDone(stepIndex int) map[string]any {
	return map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": stepIndex,
			"step_type":  "agent_response",
			"state":      "DONE",
		},
	}
}

func newToolActive(stepIndex int, toolName string, params map[string]any) map[string]any {
	return map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": stepIndex,
			"step_type":  "tool",
			"state":      "ACTIVE",
			"tool_name":  toolName,
			"tool_info": map[string]any{
				"parameters": params,
			},
		},
	}
}

func newToolDone(stepIndex int, toolName, output string, duration float64) map[string]any {
	return map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index":       stepIndex,
			"step_type":        "tool",
			"state":            "DONE",
			"tool_name":        toolName,
			"output":           output,
			"duration_seconds": duration,
		},
	}
}

func newToolError(stepIndex int, toolName, errMsg string) map[string]any {
	return map[string]any{
		"event": "step_update",
		"step_update": map[string]any{
			"step_index": stepIndex,
			"step_type":  "tool",
			"state":      "ERROR",
			"tool_name":  toolName,
			"tool_info": map[string]any{
				"error": map[string]any{
					"type":    "TOOL_ERROR",
					"message": errMsg,
				},
			},
		},
	}
}

func newResult(conversationID, response string, usage map[string]any) map[string]any {
	return map[string]any{
		"event":           "result",
		"conversation_id": conversationID,
		"result": map[string]any{
			"response": response,
			"status":   "SUCCESS",
			"usage":    usage,
		},
	}
}

func newResultWithError(conversationID, errMsg string, usage map[string]any) map[string]any {
	return map[string]any{
		"event":           "result",
		"conversation_id": conversationID,
		"result": map[string]any{
			"status": "ERROR",
			"error":  errMsg,
			"usage":  usage,
		},
	}
}

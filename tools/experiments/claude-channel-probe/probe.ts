#!/usr/bin/env bun
/**
 * Disposable channel probe for Avenor's claude-channel backend.
 *
 * Starts an MCP stdio server that:
 * - declares experimental claude/channel capability
 * - exposes avenor_report, avenor_finish, avenor_reply
 * - pushes channel events from a local HTTP endpoint
 *
 * This is script-only; do not import from production code.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// --- HTTP listener for inbound push events ---

const PORT = parseInt(process.env.AVENOR_PROBE_PORT ?? "8799", 10);

const mcp = new Server(
  { name: "avenor-probe", version: "0.0.1" },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions:
      "Messages arrive as <channel source=\"avenor-probe\">. " +
      "Reply by calling avenor_reply with the control message ID.",
  }
);

// --- Tools exposed only to the controlled Claude session ---

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: "avenor_report",
      description: "Report progress back to Avenor",
      inputSchema: {
        type: "object",
        properties: {
          state: {
            type: "string",
            enum: [
              "started",
              "thinking",
              "working",
              "blocked",
              "checkpoint",
              "permission_requested",
              "error",
            ],
          },
          payload: { type: "object" },
        },
        required: ["state", "payload"],
      },
    },
    {
      name: "avenor_finish",
      description: "Signal run completion",
      inputSchema: {
        type: "object",
        properties: {
          status: { type: "string", enum: ["done", "failed", "blocked"] },
          summary: { type: "string" },
          files_changed: { type: "array", items: { type: "string" } },
          details: { type: "object" },
        },
        required: ["status", "summary"],
      },
    },
    {
      name: "avenor_reply",
      description: "Reply to a specific control message",
      inputSchema: {
        type: "object",
        properties: {
          to: { type: "string" },
          payload: { type: "object" },
        },
        required: ["to"],
      },
    },
  ],
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  if (req.params.name === "avenor_report" || req.params.name === "avenor_finish" || req.params.name === "avenor_reply") {
    console.error(`PROBE_TOOL_CALL: ${req.params.name} ${JSON.stringify(req.params.arguments)}`);
    return { content: [{ type: "text", text: "ok" }] };
  }
  throw new Error(`unknown tool: ${req.params.name}`);
});

// --- Stdio transport (spawned by Claude Code) ---

await mcp.connect(new StdioServerTransport());

// --- HTTP listener for external push events ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    if (req.method !== "POST") {
      return new Response("method not allowed", { status: 405 });
    }
    const body = await req.text();
    console.error(`PROBE_PUSH: ${body}`);
    await mcp.notification({
      method: "notifications/claude/channel",
      params: {
        content: body,
        meta: { run_id: "probe-run-1", ctrl_id: "ctrl_001" },
      },
    });
    return new Response("ok");
  },
});

console.error(`avenor-probe listening on http://127.0.0.1:${PORT}`);

#!/usr/bin/env bun
/**
 * Avenor Claude Channel MCP Sidecar
 *
 * Spawned by Claude Code as a stdio MCP server (one per controlled session).
 * Registers with the Avenor localhost broker and forwards channel events
 * into the Claude session.
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

// --- CLI args ---

function getArg(name: string, fallback = ""): string {
  const idx = process.argv.indexOf(name);
  return idx !== -1 && idx + 1 < process.argv.length ? process.argv[idx + 1] : fallback;
}

const RUN_ID   = getArg("--run-id");
const TOKEN    = getArg("--token");
const BROKER_URL = getArg("--broker-url");

if (!RUN_ID || !TOKEN || !BROKER_URL) {
  console.error("Usage: avenor claude-channel --run-id <id> --token <token> --broker-url <url>");
  process.exit(1);
}

// --- Broker helpers ---

async function brokerPost(path: string, body: unknown): Promise<any> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, run_id: RUN_ID, token: TOKEN }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Broker ${path}: ${res.status} ${text}`);
  }
  return res.json().catch(() => null);
}

async function brokerPushControl(content: string, meta: Record<string, string>) {
  // Not used here; push-control is from Avenor to broker
}

async function brokerReport(state: string, payload: unknown) {
  await brokerPost("/report", { state, payload });
}

async function brokerFinish(status: string, summary: string, details: unknown) {
  await brokerPost("/finish", { status, summary, details });
}

async function brokerReply(to: string, payload: unknown) {
  await brokerPost("/reply", { to, payload });
}

// --- MCP Server ---

const mcp = new Server(
  { name: "avenor", version: "0.0.1" },
  {
    capabilities: {
      experimental: {
        "claude/channel": {},
        "claude/channel/permission": {},
      },
      tools: {},
    },
    instructions:
      "Messages arrive as <channel source=\"avenor\" ...>. " +
      "Reply by calling avenor_reply. Progress by calling avenor_report. " +
      "Finish by calling avenor_finish.",
  }
);

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
              "started", "thinking", "working", "blocked",
              "checkpoint", "permission_requested", "error",
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
  const name = req.params.name;
  const args = req.params.arguments as any;

  if (name === "avenor_report") {
    await brokerReport(args.state, args.payload);
    return { content: [{ type: "text", text: "ok" }] };
  }
  if (name === "avenor_finish") {
    await brokerFinish(args.status, args.summary, {
      files_changed: args.files_changed,
      details: args.details,
    });
    return { content: [{ type: "text", text: "ok" }] };
  }
  if (name === "avenor_reply") {
    await brokerReply(args.to, args.payload);
    return { content: [{ type: "text", text: "ok" }] };
  }

  throw new Error(`unknown tool: ${name}`);
});

// --- Permission relay ---

// Claude Code sends notifications/claude/channel/permission_request
// when a tool approval prompt opens.
mcp.setNotificationHandler(
  {
    method: "notifications/claude/channel/permission_request" as any,
    params: {} as any,
  },
  async (req: any) => {
    const p = req.params;
    await brokerPost("/permission_request", {
      request_id: p.request_id,
      tool_name: p.tool_name,
      description: p.description,
      input_preview: p.input_preview,
    });
  }
);

// --- Lifecycle ---

async function register() {
  const res = await brokerPost("/register", {});
  if (!res || res.token !== TOKEN) {
    console.error("Broker registration failed or token mismatch");
    process.exit(1);
  }
  console.error("avenor-sidecar: registered with broker");
}

async function heartbeatLoop() {
  while (true) {
    await new Promise((r) => setTimeout(r, 5000));
    try {
      await brokerPost("/heartbeat", {});
    } catch (e) {
      console.error("heartbeat failed:", e);
    }
  }
}

async function pollControlLoop() {
  while (true) {
    try {
      const msgs = await brokerPost("/poll-control", {});
      if (Array.isArray(msgs) && msgs.length > 0) {
        for (const msg of msgs) {
          const content = renderControlMessage(msg);
          await mcp.notification({
            method: "notifications/claude/channel",
            params: {
              content,
              meta: { run_id: msg.run_id, ctrl_id: msg.id, type: msg.type },
            },
          });
        }
      }
    } catch (e) {
      console.error("poll-control failed:", e);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
}

function renderControlMessage(msg: any): string {
  const lines = [
    `Control message ${msg.id} from Avenor:`,
    `type=${msg.type}`,
    `run_id=${msg.run_id}`,
    `payload_json=${JSON.stringify(msg.payload ?? {})}`,
  ];

  switch (msg.type) {
    case "continue":
      lines.push("Continue working on the supplied task. Use avenor_report for progress.");
      break;
    case "add_context":
      lines.push("Incorporate the new context into the current task.");
      break;
    case "request_status":
      lines.push(`Reply by calling avenor_reply with to="${msg.id}".`);
      break;
    case "cancel":
      lines.push("Stop work. Call avenor_finish(status=blocked|failed) if possible. Avoid starting new tool calls.");
      break;
    case "permission_decision":
      lines.push(`Permission decision: ${JSON.stringify(msg.payload)}`);
      break;
    default:
      lines.push(`Unhandled control type: ${msg.type}`);
  }
  return lines.join("\n");
}

// --- Start ---

await register();
await mcp.connect(new StdioServerTransport());
console.error("avenor-sidecar: MCP stdio connected");

// Run loops concurrently
heartbeatLoop();
pollControlLoop();

#!/usr/bin/env bun
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { Hono } from 'hono'
import { z } from 'zod'
import {
  spawnTool,
  statusTool,
  answerPermissionTool,
  followUpTool,
  eventsTool,
  shutdownTool,
} from '@dougbots/avenor-core'

const server = new McpServer({ name: 'avenor', version: '0.1.0' })

server.registerTool('avenor_spawn', {
  description: 'Spawn a new agent run',
  inputSchema: {
    agent: z.string().describe('Agent name (codex, claude, gemini, butler)'),
    repo_dir: z.string().describe('Working directory for the agent'),
    prompt: z.string().optional().describe('Prompt text'),
    prompt_file: z.string().optional().describe('Path to a prompt file'),
    label: z.string().optional().describe('Human-readable label for this run'),
    timeout: z.string().optional().describe('Timeout duration (e.g. 5m, 1h)'),
    model: z.string().optional().describe('Model override'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ repo_dir, agent, prompt, prompt_file, label, timeout, model, supervisor_id }) => {
  return spawnTool({
    agent,
    dir: repo_dir,
    prompt,
    promptFile: prompt_file,
    label,
    timeout,
    model,
    supervisorId: supervisor_id,
  })
})

server.registerTool('avenor_status', {
  description: 'Get status of one or all runs',
  inputSchema: {
    run_id: z.string().optional().describe('Run ID or label to query'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, supervisor_id }) => {
  return statusTool({ runId: run_id, supervisorId: supervisor_id })
})

server.registerTool('avenor_answer_permission', {
  description: 'Answer a pending permission request',
  inputSchema: {
    run_id: z.string().describe('Run ID or label'),
    option_id: z.string().describe('Option ID to select'),
    request_id: z.string().optional().describe('Permission request ID (auto-detected if omitted)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, option_id, request_id, supervisor_id }) => {
  return answerPermissionTool({
    runId: run_id,
    optionId: option_id,
    requestId: request_id,
    supervisorId: supervisor_id,
  })
})

server.registerTool('avenor_follow_up', {
  description: 'Resume a completed run with a follow-up message',
  inputSchema: {
    run_id: z.string().describe('Run ID or label to follow up on'),
    message: z.string().describe('Follow-up message'),
    label: z.string().optional().describe('Label for the follow-up run'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, message, label, supervisor_id }) => {
  return followUpTool({ runId: run_id, message, label, supervisorId: supervisor_id })
})

server.registerTool('avenor_events', {
  description: 'Get event log for a run',
  inputSchema: {
    run_id: z.string().describe('Run ID or label'),
    types: z.array(z.string()).optional().describe('Filter by event types'),
    limit: z.number().optional().describe('Max events to return (default 50)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, types, limit, supervisor_id }) => {
  return eventsTool({ runId: run_id, types, limit, supervisorId: supervisor_id })
})

server.registerTool('avenor_shutdown', {
  description: 'Shut down the supervisor',
  inputSchema: {
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
    force: z.boolean().optional().describe('Force shutdown instead of graceful'),
  },
}, async ({ supervisor_id, force }) => {
  return shutdownTool({ supervisorId: supervisor_id, force })
})

if (process.env.MCP_TRANSPORT !== 'sse') {
  const transport = new StdioServerTransport()
  await server.connect(transport)
} else {
  const port = Number(process.env.MCP_PORT ?? 3748)
  const app = new Hono()
  const transport = new StreamableHTTPServerTransport({
    sessionIdGenerator: () => crypto.randomUUID(),
  })
  app.all('/mcp', (c) => transport.handleRequest(c.req.raw, c.res, c.req.raw.body))
  await server.connect(transport)
  Bun.serve({ fetch: app.fetch, port, hostname: '127.0.0.1' })
  console.error(`avenor MCP server listening on :${port}`)
}

#!/usr/bin/env bun
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { Hono } from 'hono'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema.js'
import { routerAdmittedSchema } from './router-schema.js'
import messageContract from '../../../schemas/message.input.json'
import peersContract from '../../../schemas/peers.input.json'
import {
  spawnTool,
  statusTool,
  resultTool,
  answerPermissionTool,
  eventsTool,
  killTool,
  shutdownTool,
  createHostMessageProvider,
  getSupervisorClient,
  messageTool,
  peersDetails,
  peersTool,
  renderPeerList,
  resultOutput,
  ToolError,
  workflowCancelTool,
  workflowStatusTool,
  workflowWaitTool,
  workflowInspectTool,
  workflowEventsTool,
  workflowCompleteTool,
  workflowGateTool,
  workflowControllerStatusTool,
  type MessageProvider,
  type PeersRouterResult,
  validateSpawnSelection,
} from '@dougbots/avenor-core'

const LOCALHOST_ORIGINS = new Set(['localhost', '127.0.0.1', '[::1]'])

export function parseBearerToken(authorizationHeader: string | null | undefined): string | null {
  if (!authorizationHeader) return null
  const match = authorizationHeader.match(/^Bearer\s+(.+)$/i)
  return match?.[1]?.trim() || null
}

export function isAllowedOrigin(originHeader: string | null | undefined): boolean {
  if (!originHeader) return true
  try {
    const origin = new URL(originHeader)
    return origin.protocol === 'http:' && LOCALHOST_ORIGINS.has(origin.hostname)
  } catch {
    return false
  }
}

export function isAllowedHost(hostHeader: string | null | undefined, port: number): boolean {
  if (!hostHeader) return false
  const [host, hostPort] = hostHeader.startsWith('[')
    ? (() => {
        const end = hostHeader.indexOf(']')
        if (end === -1) return [hostHeader, undefined]
        const host = hostHeader.slice(0, end + 1)
        const portPart = hostHeader.slice(end + 1)
        return [host, portPart.startsWith(':') ? portPart.slice(1) : undefined]
      })()
    : hostHeader.split(':')

  if (!LOCALHOST_ORIGINS.has(host)) return false
  return hostPort === String(port)
}

export function getMcpAuthToken(): string {
  const token = process.env.MCP_AUTH_TOKEN?.trim()
  if (!token) {
    throw new Error('MCP_TRANSPORT=sse requires MCP_AUTH_TOKEN to be set')
  }
  return token
}

export const server = new McpServer({ name: 'avenor', version: '0.1.0' })

server.registerTool('spawn', {
  description: 'Spawn a new agent run with an optional canonical thinking level; unsupported backends reject explicit values',
  inputSchema: spawnInputShape,
}, async ({
  repo_dir,
  agent,
  prompt,
  prompt_file,
  label,
  timeout,
  model,
  thinking,
  backend,
  roster_file,
  roster_entry,
  server_url,
  supervisor_id,
}) => {
  validateSpawnSelection({
    agent,
    model,
    backend,
    roster_file,
    roster_entry,
  })

  return spawnTool({
    agent,
    dir: repo_dir,
    prompt,
    promptFile: prompt_file,
    label,
    timeout,
    model,
    thinking,
    backend,
    rosterFile: roster_file,
    rosterEntry: roster_entry,
    serverUrl: server_url,
    supervisorId: supervisor_id,
  })
})

server.registerTool('status', {
  description: 'Get lifecycle status of one or all runs. Use lifecycle view for compact polling. Without run_id, returns an object with runs (array of status objects) and count.',
  inputSchema: {
    run_id: z.string().optional().describe('Run ID or label to query'),
    view: z.enum(['lifecycle', 'full']).optional().describe('Response detail (default full for compatibility)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, view, supervisor_id }) => {
  return statusTool({ runId: run_id, supervisorId: supervisor_id, view })
})

server.registerTool('result', {
  description: 'Retrieve the complete final output of a run without transcript or event details. Waits by default; set wait=false to retrieve an already-completed run without blocking',
  inputSchema: {
    run_id: z.string().describe('Run ID or label'),
    wait: z.boolean().optional().describe('Wait for a terminal result (default true); false retrieves the current state without blocking'),
    timeout: z.string().optional().describe('Maximum time to wait (e.g. 30s, 5m)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, wait, timeout, supervisor_id }) => {
  return resultTool({ runId: run_id, supervisorId: supervisor_id, wait, timeout })
})

server.registerTool('answer_permission', {
  description: 'Answer a pending permission request',
  inputSchema: {
    run_id: z.string().describe('Run ID or label'),
    option_id: z.string().describe('Option ID to select'),
    request_id: z.string().optional().describe('Permission request ID (auto-detected if omitted)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
    message: z.string().optional().describe('Optional write-in response text. Required only when the selected option declares requiresMessage: true.'),
  },
}, async ({ run_id, option_id, request_id, supervisor_id, message }) => {
  return answerPermissionTool({
    runId: run_id,
    optionId: option_id,
    requestId: request_id,
    supervisorId: supervisor_id,
    message,
  })
})

/**
 * The message and peers tools publish the shared contract's per-action union
 * schema but admit through the shared router: admission belongs to the router
 * on every surface, so the SDK's own validation must not answer first.
 */
const messageInputSchema = routerAdmittedSchema(messageContract)
const peersInputSchema = routerAdmittedSchema(peersContract)

/**
 * The bound host provider: one supervisor and one generation at a time. It is
 * rebuilt when the supervisor's client changes (a shutdown followed by a fresh
 * singleton), so a revoked generation is never reused. This surface never
 * publishes a turn of its own, so the provider has no `recordLastTurn` caller.
 */
let hostProvider: { client: unknown; provider: MessageProvider } | undefined

async function messageProvider(): Promise<MessageProvider> {
  const { client, supervisorId } = await getSupervisorClient(undefined, { owner: true })
  if (hostProvider && hostProvider.client === client) return hostProvider.provider
  const preflight = await client.messagePreflight()
  const provider = createHostMessageProvider({
    surface: 'mcp-ts',
    supervisorId,
    generation: preflight.generation,
    client,
  })
  hostProvider = { client, provider }
  return provider
}

type ToolReply = {
  content: Array<{ type: 'text'; text: string }>
  structuredContent?: Record<string, unknown>
  isError?: true
}

/** A router tool error is a failure result on every surface, never a success. */
function toolFailure(error: unknown): ToolReply {
  const text = error instanceof Error ? error.message : String(error)
  const code = error instanceof ToolError ? error.code : 'refused'
  return {
    content: [{ type: 'text', text }],
    structuredContent: { code, text },
    isError: true,
  }
}

async function messageCall(args: unknown): Promise<ToolReply> {
  try {
    const result = await messageTool(args, await messageProvider())
    const output = resultOutput(result)
    return {
      content: [{ type: 'text', text: JSON.stringify(output) }],
      structuredContent: output,
    }
  } catch (error) {
    return toolFailure(error)
  }
}

function peersText(result: PeersRouterResult): string {
  return result.action === 'list'
    ? [result.text, renderPeerList(result.peers)].filter(part => part !== '').join('\n\n')
    : result.text
}

async function peersCall(args: unknown): Promise<ToolReply> {
  try {
    const result = await peersTool(args, await messageProvider())
    return {
      content: [{ type: 'text', text: peersText(result) }],
      structuredContent: peersDetails(result),
    }
  } catch (error) {
    return toolFailure(error)
  }
}

server.registerTool('message', {
  description: 'Send a message to another agent run, withdraw a pending ask, resume a completed run, or deliver a control reply. Every action is explicit and the result states whether a failed ask can still be withdrawn. `reply` always reports edge-absent here: this server receives no inbound asks, so nothing is sent.',
  inputSchema: messageInputSchema,
}, async args => messageCall(args))

server.registerTool('peers', {
  description: "List the visible agent sessions, rename a run, or read a run's last agent message. `last` returns the target's final text with its stop reason, or an empty result when the target has no completed turn yet.",
  inputSchema: peersInputSchema,
}, async args => peersCall(args))

server.registerTool('workflow_cancel', {
  description: 'Cancel a workflow instance. The actor and reason are required.',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    actor: z.string().describe('Who is cancelling'),
    reason: z.string().describe('Why the workflow is being cancelled'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ workflow_id, actor, reason, supervisor_id }) => {
  return workflowCancelTool({ workflow_id, actor, reason, supervisor_id })
})

server.registerTool('events', {
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

server.registerTool('kill', {
  description: 'Terminate a run without shutting down the supervisor. Other runs and the supervisor itself keep running.',
  inputSchema: {
    run_id: z.string().min(1).describe('Run ID or label'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, supervisor_id }) => {
  return killTool({ runId: run_id, supervisorId: supervisor_id })
})

server.registerTool('shutdown', {
  description: 'Shut down the supervisor',
  inputSchema: {
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
    force: z.boolean().optional().describe('Force shutdown instead of graceful'),
  },
}, async ({ supervisor_id, force }) => {
  return shutdownTool({ supervisorId: supervisor_id, force })
})

server.registerTool('workflow_status', {
  description: 'Get lightweight status for a workflow instance',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ workflow_id, supervisor_id }) => {
  return workflowStatusTool({ workflowId: workflow_id, supervisorId: supervisor_id })
})

server.registerTool('workflow_wait', {
  description: 'Wait for a workflow to reach a terminal state or until timeout',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    timeout: z.string().optional().describe('Timeout (e.g. 30s, 5m)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ workflow_id, timeout, supervisor_id }) => {
  return workflowWaitTool({ workflowId: workflow_id, timeout, supervisorId: supervisor_id })
})

server.registerTool('workflow_inspect', {
  description: 'Return the full instance detail for a workflow',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ workflow_id, supervisor_id }) => {
  return workflowInspectTool({ workflowId: workflow_id, supervisorId: supervisor_id })
})

server.registerTool('workflow_events', {
  description: 'Read log events from a workflow instance\'s event log',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    after_seq: z.number().optional().describe('Only include events after this sequence number'),
    limit: z.number().optional().describe('Max events to return (default 50)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ workflow_id, after_seq, limit, supervisor_id }) => {
  return workflowEventsTool({ workflowId: workflow_id, afterSeq: after_seq, limit, supervisorId: supervisor_id })
})

server.registerTool('workflow_complete', {
  description: 'Atomically complete a machine/external handoff activation',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    node_id: z.string().describe('Node ID'),
    activation_id: z.string().describe('Activation ID'),
    attempt_id: z.string().describe('Attempt ID'),
    lease_id: z.string().describe('Lease ID'),
    owner_token: z.string().describe('Owner token'),
    outcome: z.string().describe('Outcome'),
    outputs: z.unknown().optional().describe('Outputs'),
    artifacts: z.unknown().optional().describe('Artifacts'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async (args) => {
  const { supervisor_id, ...rest } = args
  return workflowCompleteTool({ ...rest, supervisorId: supervisor_id })
})

server.registerTool('workflow_gate', {
  description: 'Record a gate decision on a parked awaiting_gate activation',
  inputSchema: {
    workflow_id: z.string().describe('Workflow ID'),
    node_id: z.string().describe('Node ID'),
    gate_id: z.string().describe('Gate ID'),
    activation_id: z.string().describe('Activation ID'),
    operation: z.enum(['satisfy','reject','waive','external_result']).describe('Gate operation'),
    actor: z.string().optional().describe('Actor'),
    reason: z.string().optional().describe('Reason'),
    outcome: z.string().optional().describe('Outcome'),
    subject: z.unknown().optional().describe('Subject'),
    poll_id: z.string().optional().describe('Poll ID'),
    source: z.string().optional().describe('Source'),
    result: z.string().optional().describe('Result'),
    response_hash: z.string().optional().describe('Response hash'),
    observed_at: z.string().optional().describe('Observed at'),
    evidence_ids: z.array(z.string()).optional().describe('Evidence IDs'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async (args) => {
  const { supervisor_id, ...rest } = args
  return workflowGateTool({ ...rest, supervisorId: supervisor_id })
})

server.registerTool('workflow_controller_status', {
  description: 'Show workflow controller status. With controller_id, returns that controller\'s full status; without it, lists all controllers with summary state. Create, enable, and disable are CLI-only (avenor workflow controller ...).',
  inputSchema: {
    controller_id: z.string().optional().describe('Controller ID; omit to list all controllers'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ controller_id, supervisor_id }) => {
  return workflowControllerStatusTool({ controllerId: controller_id, supervisorId: supervisor_id })
})

if (import.meta.main) {
  if (process.env.MCP_TRANSPORT !== 'sse') {
    const transport = new StdioServerTransport()
    await server.connect(transport)
  } else {
    const port = Number(process.env.MCP_PORT ?? 3748)
    const authToken = getMcpAuthToken()
    const app = new Hono()
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => crypto.randomUUID(),
    })
    app.all('/mcp', (c) => {
      const request = c.req.raw
      const originHeader = request.headers.get('origin')
      const hostHeader = request.headers.get('host')
      const authHeader = request.headers.get('authorization')
      const token = parseBearerToken(authHeader)

      if (!isAllowedOrigin(originHeader)) {
        return c.text('Forbidden origin', 403)
      }

      if (!isAllowedHost(hostHeader, port)) {
        return c.text('Forbidden host', 403)
      }

      if (token !== authToken) {
        return c.text('Unauthorized', 401, {
          'WWW-Authenticate': 'Bearer',
        })
      }

      return transport.handleRequest(c.req.raw, c.res, c.req.raw.body)
    })
    await server.connect(transport)
    Bun.serve({ fetch: app.fetch, port, hostname: '127.0.0.1' })
    console.error(`avenor MCP server listening on :${port}`)
  }
}

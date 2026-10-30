#!/usr/bin/env bun
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { WebStandardStreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js'
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js'
import { Hono } from 'hono'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema.js'
import { routerAdmittedSchema } from './router-schema.js'
import messageFlatContract from '../../../schemas/message.input.flat.json'
import peersFlatContract from '../../../schemas/peers.input.flat.json'
import workflowFlatContract from '../../../schemas/workflow.input.flat.json'
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
  isGenerationMismatchError,
  messageTool,
  peersDetails,
  peersTool,
  renderPeerList,
  resultOutput,
  ToolError,
  runWorkflowTool,
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

  return passthroughResult(spawnTool({
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
  }))
})

server.registerTool('status', {
  description: 'Get lifecycle status of one or all runs. Use lifecycle view for compact polling. Without run_id, returns an object with runs (array of status objects) and count.',
  inputSchema: {
    run_id: z.string().optional().describe('Run ID or label to query'),
    view: z.enum(['lifecycle', 'full']).optional().describe('Response detail (default full for compatibility)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, view, supervisor_id }) => {
  return passthroughResult(statusTool({ runId: run_id, supervisorId: supervisor_id, view }))
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
  return passthroughResult(resultTool({ runId: run_id, supervisorId: supervisor_id, wait, timeout }))
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
  return passthroughResult(answerPermissionTool({
    runId: run_id,
    optionId: option_id,
    requestId: request_id,
    supervisorId: supervisor_id,
    message,
  }))
})

/**
 * The message and peers tools publish the flat schema compiled from the
 * shared contract's per-action union, but admit through the shared router:
 * admission belongs to the router on every surface, so the SDK's own validation
 * must not answer first. The union itself is not published here because a
 * `oneOf` input schema loses an integer's and an object's type in at least one
 * real MCP client.
 */
const messageInputSchema = routerAdmittedSchema(messageFlatContract)
const peersInputSchema = routerAdmittedSchema(peersFlatContract)
const workflowInputSchema = routerAdmittedSchema(workflowFlatContract)

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

/**
 * Run one call through the bound provider, retrying once after a re-preflight
 * when the supervisor says the cached generation is stale. This is the policy
 * the Go host already applies inside a single call, so a restarted supervisor
 * does not fail the call that discovers it.
 */
async function withProvider<T>(run: (provider: MessageProvider) => Promise<T>): Promise<T> {
  try {
    return await run(await messageProvider())
  } catch (error) {
    if (!isGenerationMismatchError(error)) throw error
    hostProvider = undefined
    return run(await messageProvider())
  }
}

type ToolReply = {
  content: Array<{ type: 'text'; text: string }>
  structuredContent?: Record<string, unknown>
  isError?: true
}

/**
 * The pass-through tools answer with the core tool's result object, which the
 * MCP result schema carries as free-form fields beside `content`. The schema
 * defaults `content` to an empty list, so sending it explicitly is what a
 * client already ends up with, and it keeps the handler's declared result
 * type honest without hiding the result fields.
 */
async function passthroughResult<T extends object>(result: T | Promise<T>): Promise<CallToolResult & T> {
  return { ...(await result), content: [] }
}

/** A router tool error is a failure result on every surface; anything else is the transport's to report. */
function toolFailure(error: unknown): ToolReply {
  if (!(error instanceof ToolError)) throw error
  const text = error.message
  return {
    content: [{ type: 'text', text }],
    structuredContent: { code: error.code, text },
    isError: true,
  }
}

async function messageCall(args: unknown): Promise<ToolReply> {
  try {
    const result = await withProvider(provider => messageTool(args, provider))
    const output = resultOutput(result)
    return {
      content: [{ type: 'text', text: result.text ?? '' }],
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
    const result = await withProvider(provider => peersTool(args, provider))
    return {
      content: [{ type: 'text', text: peersText(result) }],
      structuredContent: peersDetails(result),
    }
  } catch (error) {
    return toolFailure(error)
  }
}

server.registerTool('message', {
  description: 'Send a message to another agent run, withdraw a pending ask, resume a completed run, or deliver a control reply. Every action is explicit and the result states whether a failed ask can still be withdrawn. `reply` answers an inbound message: an ask this surface received is answered through its claim, and a tell is routed back to its sender by the broker. A message id with no route reports edge-absent. This tool is bound to the default supervisor and takes no `supervisor_id`.',
  inputSchema: messageInputSchema,
}, async args => messageCall(args))

server.registerTool('peers', {
  description: "List the visible agent sessions, rename a run, or read a run's last agent message. `last` returns the target's final text with its stop reason, or an empty result when the target has no completed turn yet. This tool is bound to the default supervisor and takes no `supervisor_id`.",
  inputSchema: peersInputSchema,
}, async args => peersCall(args))

server.registerTool('workflow', {
  description: "Drive a durable workflow instance: read its status, detail, events or wait for it; report controller readiness or status; or record a gate decision, complete a handoff activation, or cancel the instance. One `action` selects the verb. Read-only actions need no permission; gate, complete and cancel keep their authority checks.",
  inputSchema: workflowInputSchema,
}, async args => {
  try {
    const result = await runWorkflowTool(args)
    return passthroughResult(result.value)
  } catch (error) {
    return toolFailure(error)
  }
})

server.registerTool('kill', {
  description: 'Terminate a run without shutting down the supervisor. Other runs and the supervisor itself keep running.',
  inputSchema: {
    run_id: z.string().min(1).describe('Run ID or label'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, supervisor_id }) => {
  return passthroughResult(killTool({ runId: run_id, supervisorId: supervisor_id }))
})

server.registerTool('shutdown', {
  description: 'Shut down the supervisor',
  inputSchema: {
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
    force: z.boolean().optional().describe('Force shutdown instead of graceful'),
  },
}, async ({ supervisor_id, force }) => {
  return passthroughResult(shutdownTool({ supervisorId: supervisor_id, force }))
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
  return passthroughResult(eventsTool({ runId: run_id, types, limit, supervisorId: supervisor_id }))
})

export interface HttpServerOptions {
  authToken: string
  /** 0 binds an ephemeral port; the bound port is reported back. */
  port?: number
}

/**
 * Serve the MCP endpoint over HTTP with the transport's web-standard
 * implementation: Hono already hands out a Web `Request`, and the request
 * checks below answer before the transport sees a message.
 */
export async function startHttpServer(
  options: HttpServerOptions,
): Promise<{ port: number; stop: () => Promise<void> }> {
  const transport = new WebStandardStreamableHTTPServerTransport({
    sessionIdGenerator: () => crypto.randomUUID(),
  })
  // The Host header carries the bound port, which is only known after the
  // listener exists, so the request check reads it through this variable.
  let boundPort = options.port ?? 0
  const app = new Hono()
  app.all('/mcp', (c) => {
    const request = c.req.raw
    const originHeader = request.headers.get('origin')
    const hostHeader = request.headers.get('host')
    const token = parseBearerToken(request.headers.get('authorization'))

    if (!isAllowedOrigin(originHeader)) {
      return c.text('Forbidden origin', 403)
    }

    if (!isAllowedHost(hostHeader, boundPort)) {
      return c.text('Forbidden host', 403)
    }

    if (token !== options.authToken) {
      return c.text('Unauthorized', 401, {
        'WWW-Authenticate': 'Bearer',
      })
    }

    return transport.handleRequest(request)
  })
  await server.connect(transport)
  const http = Bun.serve({ fetch: app.fetch, port: boundPort, hostname: '127.0.0.1' })
  boundPort = http.port ?? boundPort
  return {
    port: boundPort,
    stop: async () => {
      await server.close()
      await http.stop(true)
    },
  }
}

if (import.meta.main) {
  if (process.env.MCP_TRANSPORT !== 'sse') {
    const transport = new StdioServerTransport()
    await server.connect(transport)
  } else {
    const { port } = await startHttpServer({
      authToken: getMcpAuthToken(),
      port: Number(process.env.MCP_PORT ?? 3748),
    })
    console.error(`avenor MCP server listening on :${port}`)
  }
}

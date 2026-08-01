#!/usr/bin/env bun
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { StreamableHTTPServerTransport } from '@modelcontextprotocol/sdk/server/streamableHttp.js'
import { Hono } from 'hono'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema.js'
import {
  spawnTool,
  statusTool,
  resultTool,
  answerPermissionTool,
  followUpTool,
  eventsTool,
  shutdownTool,
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

const server = new McpServer({ name: 'avenor', version: '0.1.0' })

server.registerTool('avenor_spawn', {
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
  server_url,
  supervisor_id,
}) => {
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
    serverUrl: server_url,
    supervisorId: supervisor_id,
  })
})

server.registerTool('avenor_status', {
  description: 'Get lifecycle status of one or all runs. Use lifecycle view for compact polling.',
  inputSchema: {
    run_id: z.string().optional().describe('Run ID or label to query'),
    view: z.enum(['lifecycle', 'full']).optional().describe('Response detail (default full for compatibility)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, view, supervisor_id }) => {
  return statusTool({ runId: run_id, supervisorId: supervisor_id, view })
})

server.registerTool('avenor_result', {
  description: 'Wait for a run to finish and return its complete final output without event details',
  inputSchema: {
    run_id: z.string().describe('Run ID or label to await'),
    wait: z.boolean().optional().describe('Wait for a terminal result (default true)'),
    timeout: z.string().optional().describe('Maximum time to wait (e.g. 30s, 5m)'),
    supervisor_id: z.string().optional().describe('Supervisor ID for multi-supervisor mode'),
  },
}, async ({ run_id, wait, timeout, supervisor_id }) => {
  return resultTool({ runId: run_id, supervisorId: supervisor_id, wait, timeout })
})

server.registerTool('avenor_answer_permission', {
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

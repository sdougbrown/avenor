import { describe, it, expect, mock } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { validateSpawnSelection } from '@dougbots/avenor-core'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema'
import { ToolError, renderPeerList } from '../../core/src/message.js'
import { renderLast } from '../../core/src/messaging-text.js'
import { messageTool as realMessageTool } from '../../core/src/tools/message.js'
import { peersTool as realPeersTool } from '../../core/src/tools/peers.js'
import type { MessageProvider } from '../../core/src/tools/message.js'
import conformance from '../../../schemas/message_route.conformance.json'

// Stub the core controller tools so the production registrations never dial a
// real supervisor; the stubs record the camelCase args the registrations
// forward.
const controllerCalls: {
  status: Array<Record<string, unknown>>
} = { status: [] }
const killCalls: Array<Record<string, unknown>> = []
const workflowCancelCalls: Array<Record<string, unknown>> = []

function makeProvider(overrides: Partial<MessageProvider> = {}): MessageProvider {
  return {
    caller: () => ({ kind: 'owner', runId: 'supervisor' }),
    surface: () => 'mcp-ts',
    role: () => 'host',
    resolve: async (ref: string) => ({ runId: ref, label: ref, match: 'unique' }),
    state: async () => ({ live: false, resumable: true, status: 'done' }),
    tell: async () => ({ outcome: 'sent' }),
    ask: async () => ({ outcome: 'sent' }),
    reply: async () => ({ outcome: 'replied' }),
    withdraw: async () => ({ outcome: 'withdrawn' }),
    resume: async () => ({ outcome: 'resumed', runId: 'run-2', label: 'again' }),
    rename: async () => ({ outcome: 'renamed' }),
    last: async () => ({ outcome: 'sent', hasRecord: false }),
    peers: async () => ({ peers: [] }),
    ...overrides,
  }
}

let providerFactory: () => MessageProvider = () => makeProvider()

mock.module('@dougbots/avenor-core', () => ({
  spawnTool: mock(async () => ({})),
  statusTool: mock(async () => ({ runs: [], count: 0 })),
  resultTool: mock(async () => ({})),
  answerPermissionTool: mock(async () => ({})),
  messageTool: realMessageTool,
  peersTool: realPeersTool,
  renderPeerList,
  ToolError,
  createHostMessageProvider: () => providerFactory(),
  getSupervisorClient: mock(async () => ({
    client: { messagePreflight: async () => ({ generation: 'gen-1' }) },
    supervisorId: '/tmp/avenor.sock',
    isSingleton: true,
    sup: null,
  })),
  eventsTool: mock(async () => ({})),
  killTool: mock(async (args: Record<string, unknown>) => {
    killCalls.push(args)
    return { killed: true }
  }),
  shutdownTool: mock(async () => ({})),
  workflowStatusTool: mock(async () => ({})),
  workflowWaitTool: mock(async () => ({})),
  workflowInspectTool: mock(async () => ({})),
  workflowEventsTool: mock(async () => ({})),
  workflowCompleteTool: mock(async () => ({})),
  workflowGateTool: mock(async () => ({})),
  workflowControllerStatusTool: mock(async (args: Record<string, unknown>) => {
    controllerCalls.status.push(args)
    return args.controllerId ? { state: 'enabled' } : { controllers: [] }
  }),
  workflowCancelTool: mock(async (args: Record<string, unknown>) => {
    workflowCancelCalls.push(args)
    return { cancelled: true }
  }),
  validateSpawnSelection,
}))

const { server, getMcpAuthToken, isAllowedHost, isAllowedOrigin, parseBearerToken } = await import('./mcp.js')

describe('avenor MCP server', () => {
  it('registers kill and maps snake_case to camelCase', async () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'test-client', version: '0.1.0' })
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)])

    const kill = await client.callTool({
      name: 'kill',
      arguments: { run_id: 'run-1', supervisor_id: 'sup-1' },
    })
    expect(kill).toMatchObject({ killed: true })
    expect(killCalls.at(-1)).toEqual({ runId: 'run-1', supervisorId: 'sup-1' })
    await client.close()
    await server.close()
  })

  it('registers the controller status tool on the production server and maps snake_case to camelCase', async () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'test-client', version: '0.1.0' })
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)])

    const status = await client.callTool({
      name: 'workflow_controller_status',
      arguments: { controller_id: 'ctl-1', supervisor_id: 'sup-1' },
    })
    expect(status).toBeDefined()
    expect(controllerCalls.status.at(-1)).toEqual({ controllerId: 'ctl-1', supervisorId: 'sup-1' })

    const list = await client.callTool({
      name: 'workflow_controller_status',
      arguments: { supervisor_id: 'sup-1' },
    })
    expect(list).toBeDefined()
    expect(controllerCalls.status.at(-1)).toEqual({ controllerId: undefined, supervisorId: 'sup-1' })
    await client.close()
    await server.close()
  })

  it('passes the statusTool result through as the status tool result', async () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'test-client', version: '0.1.0' })
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)])

    const result = await client.callTool({ name: 'status', arguments: {} })
    expect(result).toBeDefined()
    expect(result.runs).toEqual([])
    expect(result.count).toBe(0)
    await client.close()
    await server.close()
  })

  it('keeps direct and roster selectors as optional flat fields', () => {
    const schema = z.object(spawnInputShape)

    expect(schema.safeParse({ repo_dir: '/tmp/repo' }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', agent: 'codex' }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', model: 'sonnet' }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', backend: 'agy' }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', agent: 'codex', model: 'sonnet' }).success).toBe(true)
    expect(schema.safeParse({
      repo_dir: '/tmp/repo',
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
    }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', thinking: 'xhigh' }).success).toBe(true)
    expect(schema.safeParse({ repo_dir: '/tmp/repo', thinking: 'HIGH' }).success).toBe(false)
    expect(schema.safeParse({ agent: 'codex' }).success).toBe(false)
  })

  it('defers mixed-selector rejection to shared execution validation', () => {
    const input = {
      roster_file: '/repo/roster.json',
      roster_entry: 'planner',
      backend: 'agy',
    }

    expect(z.object(spawnInputShape).safeParse({ repo_dir: '/tmp/repo', ...input }).success).toBe(true)
    expect(() => validateSpawnSelection(input)).toThrow(
      'invalid spawn selector: direct identity fields are disabled in roster mode',
    )
  })

  it('parses bearer auth conservatively', () => {
    expect(parseBearerToken(null)).toBeNull()
    expect(parseBearerToken('Basic abc')).toBeNull()
    expect(parseBearerToken('Bearer   token-123 ')).toBe('token-123')
  })

  it('allows only local origins or no origin', () => {
    expect(isAllowedOrigin(null)).toBe(true)
    expect(isAllowedOrigin('http://localhost:3748')).toBe(true)
    expect(isAllowedOrigin('http://127.0.0.1:3748')).toBe(true)
    expect(isAllowedOrigin('http://[::1]:3748')).toBe(true)
    expect(isAllowedOrigin('https://localhost:3748')).toBe(false)
    expect(isAllowedOrigin('http://example.com')).toBe(false)
  })

  it('allows only localhost host headers on the configured port', () => {
    expect(isAllowedHost('localhost:3748', 3748)).toBe(true)
    expect(isAllowedHost('127.0.0.1:3748', 3748)).toBe(true)
    expect(isAllowedHost('[::1]:3748', 3748)).toBe(true)
    expect(isAllowedHost('localhost:4000', 3748)).toBe(false)
    expect(isAllowedHost('example.com:3748', 3748)).toBe(false)
    expect(isAllowedHost('localhost', 3748)).toBe(false)
  })

  it('requires an auth token for sse startup', () => {
    const original = process.env.MCP_AUTH_TOKEN
    delete process.env.MCP_AUTH_TOKEN

    try {
      expect(() => getMcpAuthToken()).toThrow('MCP_TRANSPORT=sse requires MCP_AUTH_TOKEN to be set')
    } finally {
      if (original === undefined) {
        delete process.env.MCP_AUTH_TOKEN
      } else {
        process.env.MCP_AUTH_TOKEN = original
      }
    }
  })
})

const contractFixture = conformance as unknown as {
  applicability: Record<string, { permitted: string[] }>
  peers_applicability: Record<string, { permitted: string[] }>
}

function fieldSet(table: Record<string, { permitted: string[] }>): string[] {
  const names = new Set<string>()
  for (const entry of Object.values(table)) {
    for (const name of entry.permitted) names.add(name)
  }
  return [...names].sort()
}

async function withClient<T>(run: (client: Client) => Promise<T>): Promise<T> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
  const client = new Client({ name: 'test-client', version: '0.1.0' })
  await Promise.all([client.connect(clientTransport), server.connect(serverTransport)])
  try {
    return await run(client)
  } finally {
    await client.close()
    await server.close()
  }
}

function listedTool(tools: Array<{ name: string; inputSchema: any }>, name: string) {
  const tool = tools.find(entry => entry.name === name)
  if (!tool) throw new Error(`tool ${name} is not registered`)
  return tool
}

describe('avenor MCP messaging tools', () => {
  it('registers message, peers, and workflow_cancel without the avenor_ prefix', async () => {
    await withClient(async (client) => {
      const { tools } = await client.listTools()
      const names = tools.map(tool => tool.name).sort()
      expect(names).toEqual([
        'answer_permission',
        'events',
        'kill',
        'message',
        'peers',
        'result',
        'shutdown',
        'spawn',
        'status',
        'workflow_cancel',
        'workflow_complete',
        'workflow_controller_status',
        'workflow_events',
        'workflow_gate',
        'workflow_inspect',
        'workflow_status',
        'workflow_wait',
      ])
    })
  })

  it('registers message and peers with the contract field sets', async () => {
    await withClient(async (client) => {
      const { tools } = await client.listTools()
      const message = listedTool(tools, 'message')
      expect(Object.keys(message.inputSchema.properties).sort()).toEqual(fieldSet(contractFixture.applicability))
      expect(message.inputSchema.properties.action.enum).toEqual(['tell', 'ask', 'reply', 'resume', 'withdraw', 'control_reply'])
      expect(message.inputSchema.properties).not.toHaveProperty('supervisor_id')

      const peers = listedTool(tools, 'peers')
      expect(Object.keys(peers.inputSchema.properties).sort()).toEqual(fieldSet(contractFixture.peers_applicability))
      expect(peers.inputSchema.properties.action.enum).toEqual(['list', 'rename', 'last'])
      expect(peers.inputSchema.properties).not.toHaveProperty('supervisor_id')
    })
  })

  it('routes a peers last through the shared provider and renders the shared template', async () => {
    providerFactory = () => makeProvider({
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({
        outcome: 'sent',
        label: 'mesh-a',
        hasRecord: true,
        finalOutput: 'OK',
        stopReason: 'end_turn',
        truncated: false,
      }),
    })
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'peers',
        arguments: { action: 'last', target: 'mesh-a' },
      })
      expect(result.content[0].text).toBe(
        renderLast('header', { run_id: '"rt_7"', label: '"mesh-a"', stop_reason: 'end_turn' }) + '\n\nOK',
      )
      expect(result.structuredContent.last).toMatchObject({ run_id: 'rt_7', runtime_id: 'rt_7', label: 'mesh-a' })
    })
  })

  it('returns unknown_target as a failure without reading the target', async () => {
    const last = mock(async () => ({ outcome: 'sent' }))
    providerFactory = () => makeProvider({ resolve: async () => ({ runId: '', match: 'none' }), last })
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'peers',
        arguments: { action: 'last', target: 'ghost' },
      })
      expect(result.isError).toBe(true)
      expect(result.content[0].text).toBe('no run matches "ghost"')
      expect(result.structuredContent).toEqual({ code: 'unknown_target', text: 'no run matches "ghost"' })
    })
    expect(last).not.toHaveBeenCalled()
  })

  it('reports a router tool error as a failure result', async () => {
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'message',
        arguments: { action: 'tell', target: 'mesh-a', message: 'hi', supervisor_id: '/tmp/sock' },
      })
      expect(result.isError).toBe(true)
      expect(result.content[0].text).toBe('invalid input for action "tell": unknown property "supervisor_id"')
      expect(result.structuredContent).toMatchObject({ code: 'invalid_request' })
    })
  })

  it('forwards the workflow cancel actor and reason', async () => {
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'workflow_cancel',
        arguments: { workflow_id: 'w-1', actor: 'operator', reason: 'superseded', supervisor_id: 'sup-a' },
      })
      expect(result).toMatchObject({ cancelled: true })
    })
    expect(workflowCancelCalls.at(-1)).toEqual({
      workflow_id: 'w-1',
      actor: 'operator',
      reason: 'superseded',
      supervisor_id: 'sup-a',
    })
  })
})

import { describe, it, expect, mock, beforeEach } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { validateSpawnSelection } from '@dougbots/avenor-core'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema'
import { ToolError, renderPeerList } from '../../core/src/message.js'
import { renderLast, renderOutcome } from '../../core/src/messaging-text.js'
import { messageTool as realMessageTool } from '../../core/src/tools/message.js'
import { peersTool as realPeersTool } from '../../core/src/tools/peers.js'
import type { MessageProvider } from '../../core/src/tools/message.js'
import messageFlatContract from '../../../schemas/message.input.flat.json'
import peersFlatContract from '../../../schemas/peers.input.flat.json'
import resultFixture from '../../../schemas/message_route.result.json'

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

/**
 * The host client the provider dials. It is a stable object per test so the
 * surface's memoisation is observable: it rebuilds the provider only when this
 * object changes, which is what a supervisor restart looks like.
 */
type HostClient = { messagePreflight: () => Promise<{ generation: string }> }

let preflightCalls = 0
let providerCalls = 0
const providerDials: Array<{ surface: string; supervisorId: string; generation: string }> = []

function makeHostClient(): HostClient {
  return {
    messagePreflight: async () => {
      preflightCalls += 1
      return { generation: `gen-${preflightCalls}` }
    },
  }
}

let hostClient: HostClient = makeHostClient()

/** Swap the provider, and the client with it, so the next call dials again. */
function useProvider(factory: () => MessageProvider): void {
  providerFactory = factory
  hostClient = makeHostClient()
}

beforeEach(() => {
  providerFactory = () => makeProvider()
  hostClient = makeHostClient()
  preflightCalls = 0
  providerCalls = 0
  providerDials.length = 0
  killCalls.length = 0
  controllerCalls.status.length = 0
  workflowCancelCalls.length = 0
})

mock.module('@dougbots/avenor-core', () => ({
  spawnTool: mock(async () => ({})),
  statusTool: mock(async () => ({ runs: [], count: 0 })),
  resultTool: mock(async () => ({})),
  answerPermissionTool: mock(async () => ({})),
  messageTool: realMessageTool,
  peersTool: realPeersTool,
  renderPeerList,
  ToolError,
  createHostMessageProvider: (options: { surface: string; supervisorId: string; generation: string }) => {
    providerCalls += 1
    providerDials.push({
      surface: options.surface,
      supervisorId: options.supervisorId,
      generation: options.generation,
    })
    return providerFactory()
  },
  getSupervisorClient: mock(async () => ({
    client: hostClient,
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
    expect(status).toMatchObject({ state: 'enabled' })
    expect(controllerCalls.status.at(-1)).toEqual({ controllerId: 'ctl-1', supervisorId: 'sup-1' })

    const list = await client.callTool({
      name: 'workflow_controller_status',
      arguments: { supervisor_id: 'sup-1' },
    })
    expect(list).toMatchObject({ controllers: [] })
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

  it('publishes the flat schemas compiled from the per-action union', async () => {
    await withClient(async (client) => {
      const { tools } = await client.listTools()
      expect(listedTool(tools, 'message').inputSchema).toEqual(messageFlatContract)
      expect(listedTool(tools, 'peers').inputSchema).toEqual(peersFlatContract)
    })
  })

  it('lists peers when the call omits the arguments object', async () => {
    useProvider(() => makeProvider({
      peers: async () => ({ peers: [{ run_id: 'rt_2', label: 'mesh-a' }] }),
    }))
    await withClient(async (client) => {
      const result: any = await client.callTool({ name: 'peers' })
      expect(result.isError).toBeFalsy()
      expect(result.content[0].text).toBe('**Active sessions:**\n  mesh-a (rt_2)')
      expect(result.structuredContent.peers).toEqual([{ run_id: 'rt_2', label: 'mesh-a' }])
    })
  })

  it('joins the lost sentence and the peer list when the transport reports a detail', async () => {
    useProvider(() => makeProvider({
      peers: async () => ({
        peers: [{ run_id: 'rt_2', label: 'mesh-a' }],
        detail: 'broker unavailable',
      }),
    }))
    await withClient(async (client) => {
      const result: any = await client.callTool({ name: 'peers', arguments: { action: 'list' } })
      expect(result.isError).toBeFalsy()
      expect(result.content[0].text).toBe(
        renderOutcome('peers_list', 'lost', { detail: 'broker unavailable' }) +
          '\n\n' +
          renderPeerList([{ run_id: 'rt_2', label: 'mesh-a' }]),
      )
    })
  })

  it('dials the host provider once for two calls on the same supervisor client', async () => {
    useProvider(() => makeProvider({
      state: async () => ({ live: true, resumable: false, status: 'running' }),
    }))
    await withClient(async (client) => {
      const list: any = await client.callTool({ name: 'peers', arguments: { action: 'list' } })
      expect(list.isError).toBeFalsy()
      const tell: any = await client.callTool({
        name: 'message',
        arguments: { action: 'tell', target: 'mesh-a', message: 'hi' },
      })
      expect(tell.isError).toBeFalsy()
    })
    expect(providerCalls).toBe(1)
    expect(preflightCalls).toBe(1)
    expect(providerDials).toEqual([
      { surface: 'mcp-ts', supervisorId: '/tmp/avenor.sock', generation: 'gen-1' },
    ])
  })

  it('rebuilds the host provider when the supervisor client changes', async () => {
    await withClient(async (client) => {
      await client.callTool({ name: 'peers', arguments: { action: 'list' } })
      hostClient = makeHostClient()
      const second: any = await client.callTool({ name: 'peers', arguments: { action: 'list' } })
      expect(second.isError).toBeFalsy()
    })
    expect(providerCalls).toBe(2)
    expect(preflightCalls).toBe(2)
  })

  it('retries a generation mismatch once with the freshly preflighted generation', async () => {
    let attempts = 0
    useProvider(() => makeProvider({
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => {
        attempts += 1
        if (attempts === 1) throw new Error('messaging: generation mismatch')
        return { outcome: 'sent', messageId: 'm-1' }
      },
    }))
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'message',
        arguments: { action: 'tell', target: 'mesh-a', message: 'hi' },
      })
      expect(result.isError).toBeFalsy()
      expect(result.structuredContent).toMatchObject({ action: 'tell', outcome: 'sent', message_id: 'm-1' })
    })
    expect(providerCalls).toBe(2)
    expect(preflightCalls).toBe(2)
    expect(providerDials.map(dial => dial.generation)).toEqual(['gen-1', 'gen-2'])
  })

  it('does not retry a failure that is not a generation mismatch', async () => {
    useProvider(() => makeProvider({
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => {
        throw new ToolError('refused', 'the broker refused the message')
      },
    }))
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'message',
        arguments: { action: 'tell', target: 'mesh-a', message: 'hi' },
      })
      expect(result.isError).toBe(true)
      expect(result.content[0].text).toBe('the broker refused the message')
      expect(result.structuredContent).toMatchObject({ code: 'refused' })
    })
    expect(providerCalls).toBe(1)
  })

  it('turns a plain error from a message verb into a coded router failure', async () => {
    useProvider(() => makeProvider({
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => {
        throw new Error('the broker hung up')
      },
    }))
    await withClient(async (client) => {
      const result: any = await client.callTool({
        name: 'message',
        arguments: { action: 'tell', target: 'mesh-a', message: 'hi' },
      })
      expect(result.isError).toBe(true)
      expect(result.content[0].text).toBe('the broker hung up')
      expect(result.structuredContent).toMatchObject({ code: 'refused' })
    })
    expect(providerCalls).toBe(1)
  })

  // The SDK's own handler renders a thrown handler error as a text-only `isError`
  // result, which is why this asserts no structured content rather than a code.
  it('leaves a pre-router failure to the transport instead of inventing a router code', async () => {
    hostClient = {
      messagePreflight: async () => {
        preflightCalls += 1
        throw new Error('supervisor unreachable')
      },
    }
    await withClient(async (client) => {
      const result: any = await client.callTool({ name: 'peers', arguments: { action: 'list' } })
      expect(result.isError).toBe(true)
      expect(result.content[0].text).toBe('supervisor unreachable')
      expect(result.structuredContent).toBeUndefined()
    })
    expect(providerCalls).toBe(0)
    expect(preflightCalls).toBe(1)
  })

  it('answers every inadmissible call with the router error, not the SDK error', async () => {
    await withClient(async (client) => {
      const cases: Array<{ name: string; args?: Record<string, unknown>; code: string }> = [
        { name: 'message', code: 'invalid_action' },
        { name: 'message', args: { action: 'bogus' }, code: 'invalid_action' },
        { name: 'message', args: { action: 'tell', target: 5, message: 'm' }, code: 'invalid_request' },
        { name: 'message', args: { action: 'ask', target: 'x', message: 'm', timeout_ms: 1.5 }, code: 'invalid_request' },
        { name: 'message', args: { action: 'tell', target: 'x', message: 'm', supervisor_id: 's' }, code: 'invalid_request' },
        { name: 'peers', args: { action: 'bogus' }, code: 'invalid_action' },
        { name: 'peers', args: { action: 5 }, code: 'invalid_action' },
        { name: 'peers', args: { action: 'last' }, code: 'invalid_request' },
      ]
      for (const testCase of cases) {
        const result: any = await client.callTool(
          testCase.args === undefined
            ? { name: testCase.name }
            : { name: testCase.name, arguments: testCase.args },
        )
        const label = `${testCase.name} ${JSON.stringify(testCase.args)}`
        expect({ label, isError: result.isError }).toEqual({ label, isError: true })
        expect({ label, code: result.structuredContent?.code }).toEqual({ label, code: testCase.code })
        expect({ label, sdk: /Input validation error/.test(result.content[0].text) }).toEqual({ label, sdk: false })
      }
    })
  })

  it('routes a peers last through the shared provider and renders the shared template', async () => {
    useProvider(() => makeProvider({
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({
        outcome: 'sent',
        label: 'mesh-a',
        hasRecord: true,
        finalOutput: 'OK',
        stopReason: 'end_turn',
        truncated: false,
      }),
    }))
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
    useProvider(() => makeProvider({ resolve: async () => ({ runId: '', match: 'none' }), last }))
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

  it('returns the shared result objects for every message and peers action', async () => {
    const expected = resultFixture as unknown as {
      message: Record<string, Record<string, unknown>>
      peers: Record<string, Record<string, unknown>>
      error: Record<string, unknown>
    }
    const liveAt = () => makeProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => ({ outcome: 'sent', messageId: 'm-1' }),
    })
    const resumableAt = () => makeProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: false, resumable: true, status: 'done' }),
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-2',
        runtimeId: 'rt-2',
        label: 'again',
        sentinelFile: '/runs/run-2/sentinel.done',
        onEvent: '/runs/run-2/events.log',
      }),
    })

    const peerListAt = () => makeProvider({
      peers: async () => ({
        peers: [
          { run_id: 'rt_2', label: 'mesh-a', kind: 'agent', status: 'running' },
          { run_id: 'rt_3' },
        ],
      }),
      rename: async () => ({ outcome: 'renamed', runId: 'rt_2', label: 'mesh-renamed', previousLabel: 'mesh-a' }),
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({ outcome: 'sent', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' }),
    })

    const cases: Array<{
      label: string
      provider: () => MessageProvider
      call: { name: string; arguments: Record<string, unknown> }
      expected: Record<string, unknown>
      text?: string
    }> = [
      {
        label: 'message tell',
        provider: liveAt,
        call: { name: 'message', arguments: { action: 'tell', target: 'mesh-a', message: 'hi' } },
        expected: expected.message.tell,
        text: expected.message.tell.text as string,
      },
      {
        label: 'message resume',
        provider: resumableAt,
        call: { name: 'message', arguments: { action: 'resume', target: 'mesh-a', message: 'again' } },
        expected: expected.message.resume,
      },
      {
        label: 'message reply',
        provider: () => makeProvider(),
        call: { name: 'message', arguments: { action: 'reply', reply_to: 'a-1', message: 'pong' } },
        expected: expected.message.reply_edge_absent,
      },
      {
        label: 'peers list',
        provider: peerListAt,
        call: { name: 'peers', arguments: { action: 'list' } },
        expected: expected.peers.list,
      },
      {
        label: 'peers rename',
        provider: peerListAt,
        call: { name: 'peers', arguments: { action: 'rename', name: 'mesh-renamed' } },
        expected: expected.peers.rename,
      },
      {
        label: 'peers last',
        provider: peerListAt,
        call: { name: 'peers', arguments: { action: 'last', target: 'mesh-a' } },
        expected: expected.peers.last,
      },
      {
        label: 'peers last with no matching run',
        provider: () => makeProvider({ resolve: async () => ({ runId: '', match: 'none' }) }),
        call: { name: 'peers', arguments: { action: 'last', target: 'ghost' } },
        expected: expected.error,
      },
    ]

    await withClient(async (client) => {
      for (const testCase of cases) {
        useProvider(testCase.provider)
        const result: any = await client.callTool(testCase.call)
        expect({ label: testCase.label, result: result.structuredContent })
          .toEqual({ label: testCase.label, result: testCase.expected })
        if (testCase.text !== undefined) {
          expect({ label: testCase.label, text: result.content[0].text })
            .toEqual({ label: testCase.label, text: testCase.text })
        }
      }
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
      supervisorId: 'sup-a',
    })
  })
})

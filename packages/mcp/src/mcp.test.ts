import { describe, it, expect, mock } from 'bun:test'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import { validateSpawnSelection } from '@dougbots/avenor-core'
import { z } from 'zod'
import { spawnInputShape } from './spawn-schema'

// Stub the core controller tools so the production registrations never dial a
// real supervisor; the stubs record the camelCase args the registrations
// forward.
const controllerCalls: {
  status: Array<Record<string, unknown>>
} = { status: [] }

mock.module('@dougbots/avenor-core', () => ({
  spawnTool: mock(async () => ({})),
  statusTool: mock(async () => ({})),
  resultTool: mock(async () => ({})),
  answerPermissionTool: mock(async () => ({})),
  followUpTool: mock(async () => ({})),
  eventsTool: mock(async () => ({})),
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
  validateSpawnSelection,
}))

const { server, getMcpAuthToken, isAllowedHost, isAllowedOrigin, parseBearerToken } = await import('./mcp.js')

describe('avenor MCP server', () => {
  it('registers the controller status tool on the production server and maps snake_case to camelCase', async () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    const client = new Client({ name: 'test-client', version: '0.1.0' })
    await Promise.all([client.connect(clientTransport), server.connect(serverTransport)])

    const status = await client.callTool({
      name: 'avenor_workflow_controller_status',
      arguments: { controller_id: 'ctl-1', supervisor_id: 'sup-1' },
    })
    expect(status).toBeDefined()
    expect(controllerCalls.status.at(-1)).toEqual({ controllerId: 'ctl-1', supervisorId: 'sup-1' })

    const list = await client.callTool({
      name: 'avenor_workflow_controller_status',
      arguments: { supervisor_id: 'sup-1' },
    })
    expect(list).toBeDefined()
    expect(controllerCalls.status.at(-1)).toEqual({ controllerId: undefined, supervisorId: 'sup-1' })
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

import { describe, it, expect } from 'bun:test'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { validateSpawnSelection } from '@dougbots/avenor-core'
import { z } from 'zod'
import { getMcpAuthToken, isAllowedHost, isAllowedOrigin, parseBearerToken } from './mcp'
import { spawnInputShape } from './spawn-schema'

describe('avenor MCP server', () => {
  it('registers all 7 tools without throwing', () => {
    const server = new McpServer({ name: 'avenor', version: '0.1.0' })

    server.registerTool('avenor_spawn', {
      description: 'Spawn a new agent run',
      inputSchema: spawnInputShape,
    }, async () => ({ run_id: 'test', label: 'test', supervisor_id: 'test' }))

    server.registerTool('avenor_status', {
      description: 'Get status',
      inputSchema: {
        run_id: z.string().optional(),
        view: z.enum(['lifecycle', 'full']).optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ run_id: 'test', label: 'test', status: 'running' }))

    server.registerTool('avenor_result', {
      description: 'Get result',
      inputSchema: {
        run_id: z.string(),
        wait: z.boolean().optional(),
        timeout: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ run_id: 'test', label: 'test', status: 'done', ready: true, output: 'done' }))

    server.registerTool('avenor_answer_permission', {
      description: 'Answer permission',
      inputSchema: {
        run_id: z.string(),
        option_id: z.string(),
        request_id: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ ok: true }))

    server.registerTool('avenor_follow_up', {
      description: 'Follow up',
      inputSchema: {
        run_id: z.string(),
        message: z.string(),
        label: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ run_id: 'test', label: 'test' }))

    server.registerTool('avenor_events', {
      description: 'Get events',
      inputSchema: {
        run_id: z.string(),
        types: z.array(z.string()).optional(),
        limit: z.number().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => [])

    server.registerTool('avenor_shutdown', {
      description: 'Shutdown',
      inputSchema: {
        supervisor_id: z.string().optional(),
        force: z.boolean().optional(),
      },
    }, async () => ({ ok: true, cleaned_up: [] }))

    server.registerTool('avenor_workflow_status', {
      description: 'Get lightweight status for a workflow instance',
      inputSchema: {
        workflow_id: z.string(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ status: 'running' }))

    server.registerTool('avenor_workflow_wait', {
      description: 'Wait for a workflow to reach a terminal state or until timeout',
      inputSchema: {
        workflow_id: z.string(),
        timeout: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ status: 'done', output: 'done' }))

    server.registerTool('avenor_workflow_inspect', {
      description: 'Return the full instance detail for a workflow',
      inputSchema: {
        workflow_id: z.string(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ workflow_id: 'test', nodes: [] }))

    server.registerTool('avenor_workflow_events', {
      description: 'Read log events from a workflow instance\'s event log',
      inputSchema: {
        workflow_id: z.string(),
        after_seq: z.number().optional(),
        limit: z.number().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ events: [] }))

    server.registerTool('avenor_workflow_complete', {
      description: 'Atomically complete a machine/external handoff activation',
      inputSchema: {
        workflow_id: z.string(),
        node_id: z.string(),
        activation_id: z.string(),
        attempt_id: z.string(),
        lease_id: z.string(),
        owner_token: z.string(),
        outcome: z.string(),
        outputs: z.unknown().optional(),
        artifacts: z.unknown().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ completed: true }))

    server.registerTool('avenor_workflow_gate', {
      description: 'Record a gate decision on a parked awaiting_gate activation',
      inputSchema: {
        workflow_id: z.string(),
        node_id: z.string(),
        gate_id: z.string(),
        activation_id: z.string(),
        operation: z.enum(['satisfy','reject','waive','external_result']),
        actor: z.string().optional(),
        reason: z.string().optional(),
        outcome: z.string().optional(),
        subject: z.unknown().optional(),
        poll_id: z.string().optional(),
        source: z.string().optional(),
        result: z.string().optional(),
        response_hash: z.string().optional(),
        observed_at: z.string().optional(),
        evidence_ids: z.array(z.string()).optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ decided: true }))

    expect(server).toBeDefined()
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

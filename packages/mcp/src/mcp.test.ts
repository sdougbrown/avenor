import { describe, it, expect } from 'bun:test'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { z } from 'zod'
import { getMcpAuthToken, isAllowedHost, isAllowedOrigin, parseBearerToken } from './mcp'

describe('avenor MCP server', () => {
  it('registers all 6 tools without throwing', () => {
    const server = new McpServer({ name: 'avenor', version: '0.1.0' })

    server.registerTool('avenor_spawn', {
      description: 'Spawn a new agent run',
      inputSchema: {
        agent: z.string(),
        repo_dir: z.string(),
        prompt: z.string().optional(),
        prompt_file: z.string().optional(),
        label: z.string().optional(),
        timeout: z.string().optional(),
        model: z.string().optional(),
        backend: z.string().optional(),
        server_url: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ run_id: 'test', label: 'test', supervisor_id: 'test' }))

    server.registerTool('avenor_status', {
      description: 'Get status',
      inputSchema: {
        run_id: z.string().optional(),
        supervisor_id: z.string().optional(),
      },
    }, async () => ({ run_id: 'test', label: 'test', status: 'running' }))

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

    expect(server).toBeDefined()
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

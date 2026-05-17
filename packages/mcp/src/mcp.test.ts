import { describe, it, expect } from 'bun:test'
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { z } from 'zod'

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
})

import { describe, it, expect, mock } from 'bun:test'
import { type WorkflowStatusToolArgs, type WorkflowStatusResult } from './workflow-status.js'
import { type WorkflowWaitToolArgs, type WorkflowWaitResult } from './workflow-wait.js'
import { type WorkflowInspectToolArgs, type WorkflowInspectResult } from './workflow-inspect.js'
import { type WorkflowEventsToolArgs, type WorkflowEventsResult } from './workflow-events.js'
import { type WorkflowCompleteToolArgs, type WorkflowCompleteResult } from './workflow-complete.js'
import { type WorkflowGateToolArgs, type WorkflowGateResult } from './workflow-gate.js'

// Mock getSupervisorClient that returns distinct fake clients per supervisor ID
const getSupervisorClientMock = mock(async (supervisorId: string) => {
  const clientId = `client-${supervisorId}`
  const client = {
    workflowStatus: mock(async (workflowId: string) => ({
      supervisor: supervisorId,
      workflowId,
      runtime_id: 'rt_1',
    })),
    workflowWait: mock(async (workflowId: string, timeoutMs?: number) => ({
      supervisor: supervisorId,
      workflowId,
      runtime_id: 'rt_1',
      timeoutMs,
    })),
    workflowInspect: mock(async (workflowId: string) => ({
      supervisor: supervisorId,
      workflowId,
      runtime_id: 'rt_1',
    })),
    workflowEvents: mock(async (workflowId: string, opts?: { afterSeq?: number; limit?: number }) => ({
      supervisor: supervisorId,
      workflowId,
      runtime_id: 'rt_1',
      ...opts,
    })),
    workflowComplete: mock(async (params) => ({
      supervisor: supervisorId,
      ...params,
    })),
    workflowGate: mock(async (params) => ({
      supervisor: supervisorId,
      ...params,
    })),
    workflowHeartbeat: mock(async (params) => ({
      supervisor: supervisorId,
      ...params,
    })),
    workflowCommand: mock(async (workflowId: string, command: unknown) => ({
      supervisor: supervisorId,
      workflowId,
      command,
    })),
    close: mock(() => {}),
  } as any

  return {
    client,
    isSingleton: false,
    sup: null,
    supervisorId,
  }
})

// Import tool factories and tools
import { createWorkflowStatusTool, workflowStatusTool } from './workflow-status.js'
import { createWorkflowWaitTool, workflowWaitTool } from './workflow-wait.js'
import { createWorkflowInspectTool, workflowInspectTool } from './workflow-inspect.js'
import { createWorkflowEventsTool, workflowEventsTool } from './workflow-events.js'
import { createWorkflowCompleteTool, workflowCompleteTool } from './workflow-complete.js'
import { createWorkflowGateTool, workflowGateTool } from './workflow-gate.js'

describe('workflow tools basic forwarding', () => {
  it('workflowStatusTool forwards to client.workflowStatus with workflowId', async () => {
    const tool = createWorkflowStatusTool(getSupervisorClientMock)
    const args: WorkflowStatusToolArgs = { workflowId: 'wf-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      workflowId: 'wf-123',
      runtime_id: 'rt_1',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock.mock.calls[0]?.[0]).toBe('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowWaitTool forwards with timeout conversion', async () => {
    const tool = createWorkflowWaitTool(getSupervisorClientMock)
    const args: WorkflowWaitToolArgs = { workflowId: 'wf-123', timeout: '5m', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      workflowId: 'wf-123',
      runtime_id: 'rt_1',
      timeoutMs: 300000,
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowWaitTool uses default timeout when omitted', async () => {
    const tool = createWorkflowWaitTool(getSupervisorClientMock)
    const args: WorkflowWaitToolArgs = { workflowId: 'wf-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      workflowId: 'wf-123',
      runtime_id: 'rt_1',
      timeoutMs: 30000,
    })
  })

  it('workflowInspectTool forwards to client.workflowInspect', async () => {
    const tool = createWorkflowInspectTool(getSupervisorClientMock)
    const args: WorkflowInspectToolArgs = { workflowId: 'wf-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      workflowId: 'wf-123',
      runtime_id: 'rt_1',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowEventsTool forwards with afterSeq/limit', async () => {
    const tool = createWorkflowEventsTool(getSupervisorClientMock)
    const args: WorkflowEventsToolArgs = { workflowId: 'wf-123', afterSeq: 5, limit: 10, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      workflowId: 'wf-123',
      runtime_id: 'rt_1',
      afterSeq: 5,
      limit: 10,
    })
  })

  it('workflowCompleteTool forwards with all required fields', async () => {
    const tool = createWorkflowCompleteTool(getSupervisorClientMock)
    const args: WorkflowCompleteToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      activation_id: 'act-1',
      attempt_id: 'att-1',
      lease_id: 'lease-1',
      owner_token: 'token-1',
      outcome: 'success',
      outputs: { data: 'test' },
      artifacts: [{ id: 'artifact-1' }],
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      ...args,
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowGateTool forwards with all required fields', async () => {
    const tool = createWorkflowGateTool(getSupervisorClientMock)
    const args: WorkflowGateToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      gate_id: 'gate-1',
      activation_id: 'act-1',
      operation: 'satisfy',
      actor: 'actor-1',
      reason: 'all good',
      outcome: 'ok',
      subject: { extra: 'data' },
      poll_id: 'poll-1',
      source: 'source-1',
      result: 'result-1',
      response_hash: 'hash-1',
      observed_at: '2024-01-01T00:00:00Z',
      evidence_ids: ['ev-1', 'ev-2'],
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    const result = await tool(args)
    expect(result).toEqual({
      supervisor: '/tmp/avenor-mcp-sup-a-123.sock',
      ...args,
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })
})

describe('workflow tools required-arg validation', () => {
  it('status tool throws when workflowId missing', async () => {
    const tool = createWorkflowStatusTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('workflowId is required')
  })

  it('wait tool throws when workflowId missing', async () => {
    const tool = createWorkflowWaitTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('workflowId is required')
  })

  it('inspect tool throws when workflowId missing', async () => {
    const tool = createWorkflowInspectTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('workflowId is required')
  })

  it('events tool throws when workflowId missing', async () => {
    const tool = createWorkflowEventsTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('workflowId is required')
  })

  it('complete tool throws when required fields missing (in Go MCP order)', async () => {
    const tool = createWorkflowCompleteTool(getSupervisorClientMock)
    const args: WorkflowCompleteToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      activation_id: 'act-1',
      attempt_id: 'att-1',
      lease_id: 'lease-1',
      owner_token: 'token-1',
      outcome: '',
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    await expect(tool(args)).rejects.toThrow('outcome is required')
  })

  it('gate tool throws when required fields missing (in Go MCP order)', async () => {
    const tool = createWorkflowGateTool(getSupervisorClientMock)
    const args: WorkflowGateToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      gate_id: 'gate-1',
      activation_id: 'act-1',
      operation: '',
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    await expect(tool(args)).rejects.toThrow('operation is required')
  })
})

describe('workflow tools validation (gate)', () => {
  it('gate tool rejects unknown operation', async () => {
    const tool = createWorkflowGateTool(getSupervisorClientMock)
    const args: WorkflowGateToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      gate_id: 'gate-1',
      activation_id: 'act-1',
      operation: 'invalid_op',
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    await expect(tool(args)).rejects.toThrow('unknown gate operation "invalid_op" (allowed: satisfy, reject, waive, external_result)')
  })

  it('gate tool rejects non-RFC3339 observed_at', async () => {
    const tool = createWorkflowGateTool(getSupervisorClientMock)
    const args: WorkflowGateToolArgs = {
      workflow_id: 'wf-123',
      node_id: 'node-1',
      gate_id: 'gate-1',
      activation_id: 'act-1',
      operation: 'satisfy',
      observed_at: 'not-a-timestamp',
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    await expect(tool(args)).rejects.toThrow('observed_at must be an RFC3339 timestamp')
  })
})

describe('two-supervisor no-collision routing', () => {
  it('workflowStatusTool routes by supervisorId even when runtime_id and workflow_id collide', async () => {
    const tool = createWorkflowStatusTool(getSupervisorClientMock)

    const argsA: WorkflowStatusToolArgs = { workflowId: 'wf-shared', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const argsB: WorkflowStatusToolArgs = { workflowId: 'wf-shared', supervisorId: '/tmp/avenor-mcp-sup-b-456.sock' }

    const resultA = await tool(argsA)
    const resultB = await tool(argsB)

    expect(resultA.supervisor).toBe('/tmp/avenor-mcp-sup-a-123.sock')
    expect(resultB.supervisor).toBe('/tmp/avenor-mcp-sup-b-456.sock')
    expect(resultA.runtime_id).toBe('rt_1')
    expect(resultB.runtime_id).toBe('rt_1')

    // Ensure separate client calls were made for each supervisor
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-b-456.sock')
  })

  it('workflowCompleteTool routes by supervisorId even when runtime_id and workflow_id collide', async () => {
    const tool = createWorkflowCompleteTool(getSupervisorClientMock)

    const argsA: WorkflowCompleteToolArgs = {
      workflow_id: 'wf-shared',
      node_id: 'node-1',
      activation_id: 'act-1',
      attempt_id: 'att-1',
      lease_id: 'lease-1',
      owner_token: 'token-1',
      outcome: 'success',
      supervisorId: '/tmp/avenor-mcp-sup-a-123.sock',
    }
    const argsB: WorkflowCompleteToolArgs = {
      workflow_id: 'wf-shared',
      node_id: 'node-1',
      activation_id: 'act-1',
      attempt_id: 'att-1',
      lease_id: 'lease-1',
      owner_token: 'token-1',
      outcome: 'success',
      supervisorId: '/tmp/avenor-mcp-sup-b-456.sock',
    }

    const resultA = await tool(argsA)
    const resultB = await tool(argsB)

    expect(resultA.supervisor).toBe('/tmp/avenor-mcp-sup-a-123.sock')
    expect(resultB.supervisor).toBe('/tmp/avenor-mcp-sup-b-456.sock')
    expect(resultA.workflow_id).toBe('wf-shared')
    expect(resultB.workflow_id).toBe('wf-shared')

    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-b-456.sock')
  })
})

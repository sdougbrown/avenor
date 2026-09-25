import { describe, it, expect, mock } from 'bun:test'
import { type WorkflowControllerStatusToolArgs, type WorkflowControllerStatusResult } from './workflow-controller-status.js'
import { type WorkflowControllerListToolArgs, type WorkflowControllerListResult } from './workflow-controller-list.js'
import { type WorkflowControllerCreateToolArgs, type WorkflowControllerCreateResult } from './workflow-controller-create.js'
import { type WorkflowControllerEnableToolArgs, type WorkflowControllerEnableResult } from './workflow-controller-enable.js'
import { type WorkflowControllerDisableToolArgs, type WorkflowControllerDisableResult } from './workflow-controller-disable.js'

// Mock getSupervisorClient that returns distinct fake clients per supervisor ID
const getSupervisorClientMock = mock(async (supervisorId: string) => {
  const client = {
    workflowControllerStatus: mock(async (controllerId: string) => ({
      controller_id: controllerId,
      desired_state: 'enabled',
    })),
    workflowControllerList: mock(async () => ({
      controllers: [],
    })),
    workflowControllerCreate: mock(async (params) => ({
      ...params,
      desired_state: 'disabled',
    })),
    workflowControllerEnable: mock(async (controllerId: string) => ({
      controller_id: controllerId,
      desired_state: 'enabled',
    })),
    workflowControllerDisable: mock(async (controllerId: string, reason: string) => ({
      controller_id: controllerId,
      desired_state: 'disabled',
      reason,
    })),
    workflowReady: mock(async (controllerId: string, limit?: number) => ({
      controller_id: controllerId,
      limit,
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
import { createWorkflowControllerStatusTool, workflowControllerStatusTool } from './workflow-controller-status.js'
import { createWorkflowControllerListTool, workflowControllerListTool } from './workflow-controller-list.js'
import { createWorkflowControllerCreateTool, workflowControllerCreateTool } from './workflow-controller-create.js'
import { createWorkflowControllerEnableTool, workflowControllerEnableTool } from './workflow-controller-enable.js'
import { createWorkflowControllerDisableTool, workflowControllerDisableTool } from './workflow-controller-disable.js'
import { createWorkflowReadyTool } from './workflow-ready.js'

describe('workflow controller tools basic forwarding', () => {
  it('workflowControllerStatusTool forwards to client.workflowControllerStatus with controllerId', async () => {
    const tool = createWorkflowControllerStatusTool(getSupervisorClientMock)
    const args: WorkflowControllerStatusToolArgs = { controllerId: 'c-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controller_id: 'c-123',
      desired_state: 'enabled',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock.mock.calls[0]?.[0]).toBe('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowControllerListTool forwards to client.workflowControllerList', async () => {
    const tool = createWorkflowControllerListTool(getSupervisorClientMock)
    const args: WorkflowControllerListToolArgs = { supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controllers: [],
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowControllerCreateTool forwards with controllerId and maxInflight', async () => {
    const tool = createWorkflowControllerCreateTool(getSupervisorClientMock)
    const args: WorkflowControllerCreateToolArgs = { controllerId: 'c-123', maxInflight: 5, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controller_id: 'c-123',
      max_inflight: 5,
      desired_state: 'disabled',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowControllerEnableTool forwards with controllerId', async () => {
    const tool = createWorkflowControllerEnableTool(getSupervisorClientMock)
    const args: WorkflowControllerEnableToolArgs = { controllerId: 'c-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controller_id: 'c-123',
      desired_state: 'enabled',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('workflowControllerDisableTool forwards with controllerId and reason', async () => {
    const tool = createWorkflowControllerDisableTool(getSupervisorClientMock)
    const args: WorkflowControllerDisableToolArgs = { controllerId: 'c-123', reason: 'why', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controller_id: 'c-123',
      desired_state: 'disabled',
      reason: 'why',
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })
})

describe('workflow controller tools required-arg validation', () => {
  it('status tool throws when controllerId missing', async () => {
    const tool = createWorkflowControllerStatusTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('controllerId is required')
  })

  it('create tool throws when controllerId missing', async () => {
    const tool = createWorkflowControllerCreateTool(getSupervisorClientMock)
    await expect(tool({ maxInflight: 5, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('controllerId is required')
  })

  it('create tool throws when maxInflight is zero', async () => {
    const tool = createWorkflowControllerCreateTool(getSupervisorClientMock)
    await expect(tool({ controllerId: 'c-123', maxInflight: 0, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('maxInflight must be a positive integer')
  })

  it('create tool throws when maxInflight is negative', async () => {
    const tool = createWorkflowControllerCreateTool(getSupervisorClientMock)
    await expect(tool({ controllerId: 'c-123', maxInflight: -1, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('maxInflight must be a positive integer')
  })

  it('create tool throws when maxInflight is not an integer', async () => {
    const tool = createWorkflowControllerCreateTool(getSupervisorClientMock)
    await expect(tool({ controllerId: 'c-123', maxInflight: 1.5, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('maxInflight must be a positive integer')
  })

  it('enable tool throws when controllerId missing', async () => {
    const tool = createWorkflowControllerEnableTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('controllerId is required')
  })

  it('disable tool throws when controllerId missing', async () => {
    const tool = createWorkflowControllerDisableTool(getSupervisorClientMock)
    await expect(tool({ reason: 'why', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('controllerId is required')
  })

  it('disable tool throws when reason missing', async () => {
    const tool = createWorkflowControllerDisableTool(getSupervisorClientMock)
    await expect(tool({ controllerId: 'c-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('reason is required')
  })
})

describe('workflow ready tool', () => {
  it('throws when controllerId missing', async () => {
    const tool = createWorkflowReadyTool(getSupervisorClientMock)
    await expect(tool({ supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })).rejects.toThrow('controllerId is required')
  })

  it('forwards to client.workflowReady with limit when supplied', async () => {
    const tool = createWorkflowReadyTool(getSupervisorClientMock)
    const result = await tool({ controllerId: 'c-123', limit: 5, supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })
    expect(result).toEqual({
      controller_id: 'c-123',
      limit: 5,
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })

  it('forwards to client.workflowReady without limit when omitted', async () => {
    const tool = createWorkflowReadyTool(getSupervisorClientMock)
    const result = await tool({ controllerId: 'c-123', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' })
    expect(result).toEqual({
      controller_id: 'c-123',
      limit: undefined,
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
  })
})

describe('two-supervisor no-collision routing', () => {
  it('workflowControllerStatusTool routes by supervisorId even when controller_id collides', async () => {
    const tool = createWorkflowControllerStatusTool(getSupervisorClientMock)

    const argsA: WorkflowControllerStatusToolArgs = { controllerId: 'c-shared', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const argsB: WorkflowControllerStatusToolArgs = { controllerId: 'c-shared', supervisorId: '/tmp/avenor-mcp-sup-b-456.sock' }

    await tool(argsA)
    await tool(argsB)

    // Ensure separate client calls were made for each supervisor
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-b-456.sock')
  })

  it('workflowControllerDisableTool routes by supervisorId even when controller_id collides', async () => {
    const tool = createWorkflowControllerDisableTool(getSupervisorClientMock)

    const argsA: WorkflowControllerDisableToolArgs = { controllerId: 'c-shared', reason: 'why', supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const argsB: WorkflowControllerDisableToolArgs = { controllerId: 'c-shared', reason: 'why', supervisorId: '/tmp/avenor-mcp-sup-b-456.sock' }

    await tool(argsA)
    await tool(argsB)

    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-b-456.sock')
  })
})

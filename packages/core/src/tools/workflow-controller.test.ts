import { describe, it, expect, mock } from 'bun:test'
import { type WorkflowControllerStatusToolArgs } from './workflow-controller-status.js'

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
import { createWorkflowControllerStatusTool } from './workflow-controller-status.js'
import { createWorkflowReadyTool } from './workflow-ready.js'

describe('workflow controller status tool basic forwarding', () => {
  it('forwards to client.workflowControllerStatus when controllerId is set', async () => {
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

  it('forwards to client.workflowControllerList when controllerId is omitted', async () => {
    const tool = createWorkflowControllerStatusTool(getSupervisorClientMock)
    const args: WorkflowControllerStatusToolArgs = { supervisorId: '/tmp/avenor-mcp-sup-a-123.sock' }
    const result = await tool(args)
    expect(result).toEqual({
      controllers: [],
    })
    expect(getSupervisorClientMock).toHaveBeenCalledWith('/tmp/avenor-mcp-sup-a-123.sock')
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
})

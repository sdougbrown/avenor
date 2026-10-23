import { describe, expect, it, mock } from 'bun:test'
import { createWorkflowCancelTool } from './workflow-cancel.js'

function fakeClient() {
  const workflowCancel = mock(async (_workflowId: string, _actor: string, _reason: string) => ({ cancelled: true }))
  return {
    workflowCancel,
    getSupervisorClient: mock(async (supervisorId: string) => ({
      client: { workflowCancel, close: mock(() => {}) } as any,
      isSingleton: false,
      sup: null,
      supervisorId,
    })),
  }
}

describe('workflow cancel tool', () => {
  it('requires the workflow id, then the actor, then the reason', async () => {
    const { workflowCancel, getSupervisorClient } = fakeClient()
    const tool = createWorkflowCancelTool(getSupervisorClient)
    await expect(tool({ workflow_id: '', actor: 'a', reason: 'r' })).rejects.toThrow('workflow_id is required')
    await expect(tool({ workflow_id: 'w', actor: '', reason: 'r' })).rejects.toThrow('actor is required')
    await expect(tool({ workflow_id: 'w', actor: 'a', reason: '' })).rejects.toThrow('reason is required')
    expect(workflowCancel).not.toHaveBeenCalled()
  })

  it('forwards all three arguments to the client', async () => {
    const { workflowCancel, getSupervisorClient } = fakeClient()
    const tool = createWorkflowCancelTool(getSupervisorClient)
    const result = await tool({
      workflow_id: 'w-1',
      actor: 'operator',
      reason: 'superseded',
      supervisorId: '/tmp/avenor-mcp-sup.sock',
    })
    expect(result).toEqual({ cancelled: true })
    expect(workflowCancel).toHaveBeenCalledWith('w-1', 'operator', 'superseded')
    expect(getSupervisorClient).toHaveBeenCalledWith('/tmp/avenor-mcp-sup.sock')
  })
})

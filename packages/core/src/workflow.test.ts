import { describe, it, expect } from 'bun:test'
import conformance from '../../../schemas/workflow_route.conformance.json'
import { executeWorkflow, WorkflowToolError, workflowSupervisorId, type WorkflowAction, type WorkflowProvider } from './workflow.js'

interface ConformanceCase {
  name: string
  input: unknown
  schema?: boolean
  expect: {
    action?: WorkflowAction
    call?: string
    args?: Record<string, unknown>
    code?: string
    message?: string
  }
}

const cases = (conformance as unknown as { cases: ConformanceCase[] }).cases

interface Call {
  method: string
  args: Record<string, unknown>
}

function fakeProvider(): { provider: WorkflowProvider; calls: Call[] } {
  const calls: Call[] = []
  const record = (method: string, args: Record<string, unknown>) => {
    calls.push({ method, args })
    return {}
  }
  const provider: WorkflowProvider = {
    status: async workflowId => record('status', { workflow_id: workflowId }),
    wait: async (workflowId, timeoutMs) => record('wait', { workflow_id: workflowId, timeout_ms: timeoutMs }),
    inspect: async workflowId => record('inspect', { workflow_id: workflowId }),
    events: async (workflowId, opts) =>
      record('events', { workflow_id: workflowId, after_seq: opts?.afterSeq ?? 0, limit: opts?.limit ?? 0 }),
    complete: async params => record('complete', params as unknown as Record<string, unknown>),
    gate: async params => record('gate', params as unknown as Record<string, unknown>),
    cancel: async (workflowId, actor, reason) => record('cancel', { workflow_id: workflowId, actor, reason }),
    controllerStatus: async controllerId => record('controller_status', { controller_id: controllerId }),
    controllerList: async () => record('controller_list', {}),
    ready: async (controllerId, limit) => record('ready', { controller_id: controllerId, limit: limit ?? 0 }),
  }
  return { provider, calls }
}

describe('avenor_workflow conformance', () => {
  for (const testCase of cases) {
    it(testCase.name, async () => {
      const { provider, calls } = fakeProvider()
      if (testCase.expect.code) {
        let caught: unknown
        try {
          await executeWorkflow(provider, testCase.input)
        } catch (error) {
          caught = error
        }
        expect(caught).toBeInstanceOf(WorkflowToolError)
        const toolError = caught as WorkflowToolError
        expect(toolError.code).toBe(testCase.expect.code!)
        expect(toolError.message).toBe(testCase.expect.message!)
        expect(calls).toHaveLength(0)
        return
      }
      const result = await executeWorkflow(provider, testCase.input)
      expect(result.action).toBe(testCase.expect.action!)
      expect(calls).toHaveLength(1)
      expect(calls[0].method).toBe(testCase.expect.call!)
      expect(calls[0].args).toEqual(testCase.expect.args!)
    })
  }
})

describe('avenor_workflow admission', () => {
  it('refuses a non-object input as an invalid action', async () => {
    const { provider } = fakeProvider()
    await expect(executeWorkflow(provider, null)).rejects.toMatchObject({ code: 'invalid_action' })
    await expect(executeWorkflow(provider, [])).rejects.toMatchObject({ code: 'invalid_action' })
    await expect(executeWorkflow(provider, 'status')).rejects.toMatchObject({ code: 'invalid_action' })
  })

  it('refuses a missing action as an invalid action', async () => {
    const { provider } = fakeProvider()
    await expect(executeWorkflow(provider, { workflow_id: 'wf-1' })).rejects.toMatchObject({ code: 'invalid_action' })
  })

  it('refuses a non-string action as an invalid action', async () => {
    const { provider } = fakeProvider()
    await expect(executeWorkflow(provider, { action: 7 })).rejects.toMatchObject({ code: 'invalid_action' })
  })

  it('refuses an unknown action with the shared vocabulary sentence', async () => {
    const { provider } = fakeProvider()
    await expect(executeWorkflow(provider, { action: 'explode' })).rejects.toMatchObject({
      code: 'invalid_action',
      message: 'not a known action (status, inspect, events, wait, ready, controller_status, gate, complete, cancel)',
    })
  })
})

describe('avenor_workflow supervisor binding', () => {
  it('reads the supervisor_id a call names', () => {
    expect(workflowSupervisorId({ action: 'status', workflow_id: 'wf-1', supervisor_id: '/tmp/s.sock' })).toBe('/tmp/s.sock')
    expect(workflowSupervisorId({ action: 'status', workflow_id: 'wf-1' })).toBeUndefined()
    expect(workflowSupervisorId(null)).toBeUndefined()
    expect(workflowSupervisorId({ supervisor_id: 7 })).toBeUndefined()
  })
})

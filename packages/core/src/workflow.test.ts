import { describe, it, expect } from 'bun:test'
import conformance from '../../../schemas/workflow_route.conformance.json'
import type { Client } from './client.js'
import { runWorkflowTool } from './tools/workflow.js'
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

describe('avenor_workflow numeric and timestamp bounds', () => {
  it('accepts the maximum int64 after JSON.parse rounds it up', async () => {
    const { provider, calls } = fakeProvider()
    // JSON.parse('9223372036854775807') rounds to 2**63, so the check must not
    // refuse the largest valid int64 as out of range.
    const result = await executeWorkflow(provider, { action: 'events', workflow_id: 'wf-1', after_seq: 9223372036854775807 })
    expect(result.action).toBe('events')
    expect(calls[0].args.after_seq).toBe(9223372036854775807)
  })

  it('refuses an impossible calendar date', async () => {
    const { provider } = fakeProvider()
    await expect(
      executeWorkflow(provider, {
        action: 'gate',
        workflow_id: 'wf-1',
        node_id: 'n1',
        gate_id: 'g1',
        activation_id: 'a1',
        operation: 'satisfy',
        observed_at: '2026-13-01T00:00:00Z',
      }),
    ).rejects.toMatchObject({ code: 'invalid_request' })
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

describe('avenor_workflow surface wiring', () => {
  function fakeClient(overrides: Partial<Client>): { client: Client; closed: () => boolean } {
    let closed = false
    const client = {
      close: () => {
        closed = true
      },
      ...overrides,
    } as unknown as Client
    return { client, closed: () => closed }
  }

  it('routes the call through the supervisor client it names and closes it', async () => {
    const { client, closed } = fakeClient({
      workflowStatus: async (workflowId: string) => ({ workflow_id: workflowId, status: 'active' }),
    })
    const dialed: Array<string | undefined> = []
    const result = await runWorkflowTool(
      { action: 'status', workflow_id: 'wf-1', supervisor_id: '/tmp/s.sock' },
      async supervisorId => {
        dialed.push(supervisorId)
        return { client, isSingleton: false, sup: null, supervisorId: supervisorId ?? '' }
      },
    )
    expect(result).toEqual({ action: 'status', value: { workflow_id: 'wf-1', status: 'active' } })
    expect(dialed).toEqual(['/tmp/s.sock'])
    expect(closed()).toBe(true)
  })

  it('renders a provider failure as the shared refusal', async () => {
    const { client } = fakeClient({
      workflowStatus: async () => {
        throw Object.assign(new Error('open /home/u/.avenor/workflows/wf-1/state.json: no such file'), { code: 'ENOENT' })
      },
    })
    await expect(
      runWorkflowTool({ action: 'status', workflow_id: 'wf-1', supervisor_id: '/tmp/s.sock' }, async supervisorId => ({
        client,
        isSingleton: false,
        sup: null,
        supervisorId: supervisorId ?? '',
      })),
    ).rejects.toMatchObject({ code: 'refused', message: 'the supervisor could not be reached' })
  })

  it('passes a router denial through unchanged', async () => {
    const { client } = fakeClient({})
    await expect(
      runWorkflowTool({ action: 'status' }, async supervisorId => ({
        client,
        isSingleton: false,
        sup: null,
        supervisorId: supervisorId ?? '',
      })),
    ).rejects.toMatchObject({ code: 'invalid_request' })
  })
})

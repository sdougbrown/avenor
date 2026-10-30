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

  it('omits a gate field whose value is the empty string', async () => {
    const { provider, calls } = fakeProvider()
    await executeWorkflow(provider, {
      action: 'gate',
      workflow_id: 'wf-1',
      node_id: 'n1',
      gate_id: 'g1',
      activation_id: 'a1',
      operation: 'satisfy',
      actor: '',
      outcome: '',
      source: '',
    })
    expect(calls[0].args).not.toHaveProperty('actor')
    expect(calls[0].args).not.toHaveProperty('outcome')
    expect(calls[0].args).not.toHaveProperty('source')
  })

  it('refuses an integer below the int64 range', async () => {
    const { provider } = fakeProvider()
    await expect(
      executeWorkflow(provider, { action: 'events', workflow_id: 'wf-1', after_seq: -1e21 }),
    ).rejects.toMatchObject({ code: 'invalid_request' })
  })

  it('accepts the minimum int64', async () => {
    const { provider, calls } = fakeProvider()
    const result = await executeWorkflow(provider, { action: 'events', workflow_id: 'wf-1', after_seq: -(2 ** 63) })
    expect(result.action).toBe('events')
    expect(calls[0].args.after_seq).toBe(-(2 ** 63))
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

  it('validates the calendar date and time components', async () => {
    const gate = (observedAt: string) => ({
      action: 'gate',
      workflow_id: 'wf-1',
      node_id: 'n1',
      gate_id: 'g1',
      activation_id: 'a1',
      operation: 'satisfy',
      observed_at: observedAt,
    })
    const refused = ['2026-02-29T00:00:00Z', '2100-02-29T00:00:00Z', '2026-01-32T00:00:00Z', '2026-01-01T24:00:00Z', '2026-01-01T00:60:00Z', '2026-01-01T00:00:60Z']
    for (const observedAt of refused) {
      await expect(executeWorkflow(fakeProvider().provider, gate(observedAt))).rejects.toMatchObject({ code: 'invalid_request' })
    }
    const accepted = ['2024-02-29T00:00:00Z', '2000-02-29T00:00:00Z', '2026-01-31T23:59:59Z']
    for (const observedAt of accepted) {
      await expect(executeWorkflow(fakeProvider().provider, gate(observedAt))).resolves.toMatchObject({ action: 'gate' })
    }
  })

  it('bounds the timezone offset the way the Go parser does', async () => {
    const gate = (observedAt: string) => ({
      action: 'gate',
      workflow_id: 'wf-1',
      node_id: 'n1',
      gate_id: 'g1',
      activation_id: 'a1',
      operation: 'satisfy',
      observed_at: observedAt,
    })
    for (const observedAt of ['2026-01-01T00:00:00+25:00', '2026-01-01T00:00:00+00:61', '2026-01-01T00:00:00+99:99']) {
      await expect(executeWorkflow(fakeProvider().provider, gate(observedAt))).rejects.toMatchObject({ code: 'invalid_request' })
    }
    // Go's parser deliberately accepts 24 hours and 60 minutes.
    for (const observedAt of ['2026-01-01T00:00:00+24:00', '2026-01-01T00:00:00+00:60', '2026-01-01T00:00:00-23:59']) {
      await expect(executeWorkflow(fakeProvider().provider, gate(observedAt))).resolves.toMatchObject({ action: 'gate' })
    }
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

  it('denies a malformed call before acquiring a client', async () => {
    let dialed = false
    await expect(
      runWorkflowTool({ action: 'status' }, async supervisorId => {
        dialed = true
        return { client: fakeClient({}).client, isSingleton: false, sup: null, supervisorId: supervisorId ?? '' }
      }),
    ).rejects.toMatchObject({ code: 'invalid_request' })
    expect(dialed).toBe(false)
  })
})

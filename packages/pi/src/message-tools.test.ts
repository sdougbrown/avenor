import { describe, expect, it, mock } from 'bun:test'
import conformance from '../../../schemas/message_route.conformance.json'
import { messageTool as realMessageTool, renderLast, type MessageProvider } from '@dougbots/avenor-core'
import { makeMessageProvider, startExtensionHarness } from './test-fixtures.js'

const fixture = conformance as unknown as {
  applicability: Record<string, { permitted: string[] }>
  peers_applicability: Record<string, { permitted: string[] }>
}

function fieldSet(table: Record<string, { permitted: string[] }>): string[] {
  const names = new Set<string>()
  for (const entry of Object.values(table)) {
    for (const name of entry.permitted) names.add(name)
  }
  return [...names].sort()
}

function actionValues(schema: unknown): string[] {
  const properties = (schema as { properties: Record<string, { anyOf?: Array<{ const?: string }> }> }).properties
  const anyOf = properties.action.anyOf ?? []
  return anyOf.map(entry => entry.const ?? '').sort()
}

/** The registered parameter schema's property names and action enum. */
function registered(schema: unknown): { fields: string[], actions: string[] } {
  return {
    fields: Object.keys((schema as { properties: Record<string, unknown> }).properties).sort(),
    actions: actionValues(schema),
  }
}

describe('Pi message tool parity', () => {
  it('registers avenor_message with the contract field set in host mode', async () => {
    const harness = await startExtensionHarness({})
    const tool = harness.registeredTools.avenor_message
    const { fields, actions } = registered(tool.parameters)
    expect(fields).toEqual(fieldSet(fixture.applicability))
    expect(actions).toEqual(['ask', 'control_reply', 'reply', 'resume', 'tell', 'withdraw'])
  })

  it('registers avenor_peers with the contract field set in host mode', async () => {
    const harness = await startExtensionHarness({})
    const { fields, actions } = registered(harness.registeredTools.avenor_peers.parameters)
    expect(fields).toEqual(fieldSet(fixture.peers_applicability))
    expect(actions).toEqual(['last', 'list', 'rename'])
  })

  it('keeps both tool field sets in child mode', async () => {
    const harness = await startExtensionHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
    })
    expect(registered(harness.registeredTools.avenor_message.parameters).fields)
      .toEqual(fieldSet(fixture.applicability))
    const peers = registered(harness.registeredTools.avenor_peers.parameters)
    expect(peers.fields).toEqual(fieldSet(fixture.peers_applicability))
    expect(peers.actions).toEqual(['last', 'list', 'rename'])
  })
})

describe('Pi host turn producer', () => {
  function recordingProvider(record: ReturnType<typeof mock>): MessageProvider {
    return makeMessageProvider({ recordLastTurn: record as unknown as MessageProvider['recordLastTurn'] })
  }

  it('publishes the settled host turn', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [
      { role: 'assistant', content: [{ type: 'text', text: 'the answer' }], stopReason: 'stop' },
    ] })
    await harness.eventHandlers.agent_settled({})
    expect(record).toHaveBeenCalledWith('the answer', 'end_turn', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes an empty or cancelled settled turn instead of skipping it', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [
      { role: 'assistant', content: [], stopReason: 'aborted' },
    ] })
    await harness.eventHandlers.agent_settled({})
    expect(record).toHaveBeenCalledWith('', 'cancelled', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes the truncation flag carried from the unbounded text', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    const long = 'x'.repeat(5000)
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [
      { role: 'assistant', content: long, stopReason: 'stop' },
    ] })
    await harness.eventHandlers.agent_settled({})
    const [text, stop, truncated] = record.mock.calls[0] as [string, string, boolean]
    expect(text).toHaveLength(4096)
    expect(stop).toBe('end_turn')
    expect(truncated).toBe(true)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes once when a retry emits agent_end twice before one settle', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [{ content: 'first attempt', stopReason: 'error' }] })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [{ content: 'retry', stopReason: 'stop' }] })
    await harness.eventHandlers.agent_settled({})
    expect(record).toHaveBeenCalledTimes(1)
    expect(record).toHaveBeenCalledWith('retry', 'end_turn', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes nothing in child mode', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startExtensionHarness({
      deps: { hostProviderFor: mock(async () => recordingProvider(record)) },
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
    })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [{ content: 'child turn', stopReason: 'stop' }] })
    await harness.eventHandlers.agent_settled({})
    expect(record).not.toHaveBeenCalled()
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('logs a throwing session_last_set without rejecting the handler', async () => {
    const record = mock(async () => {
      throw new Error('broker down')
    })
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    harness.eventHandlers.agent_end({ type: 'agent_end', messages: [{ content: 'turn', stopReason: 'stop' }] })
    await expect(harness.eventHandlers.agent_settled({})).resolves.toBeUndefined()
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi resumed-run tracking', () => {
  it('tracks the resumed run on its supervisor and starts polling', async () => {
    const statusTool = mock(async (args: { runId: string }) => ({
      run_id: args.runId,
      label: 'mesh-9',
      status: 'done',
      runtime_id: 'rt-9',
    }))
    const provider = makeMessageProvider({
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-9',
        runtimeId: 'rt-9',
        label: 'mesh-9',
        sentinelFile: '/runs/run-9/sentinel.done',
        onEvent: '/runs/run-9/events.log',
      }),
      registerResumed: () => {},
    })
    const hostProviderFor = mock(async (_supervisorId?: string) => provider)
    const harness = await startExtensionHarness({
      deps: { hostProviderFor, statusTool, messageTool: realMessageTool },
    })
    const result = await harness.registeredTools.avenor_message.execute('t1', {
      action: 'resume',
      target: 'mesh-9',
      message: 'again',
    })
    expect(result.details.outcome).toBe('resumed')
    expect(statusTool).toHaveBeenCalledWith({ runId: 'run-9', supervisorId: undefined })
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi peers last', () => {
  it('resolves through the provider and renders the shared template', async () => {
    const provider = makeMessageProvider({
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({
        outcome: 'sent',
        label: 'mesh-a',
        hasRecord: true,
        finalOutput: 'OK',
        stopReason: 'end_turn',
        truncated: false,
      }),
    })
    const harness = await startExtensionHarness({ deps: { hostProviderFor: mock(async () => provider) } })
    const result = await harness.registeredTools.avenor_peers.execute('t1', { action: 'last', target: 'mesh-a' })
    expect(result.content[0].text).toBe(
      renderLast('header', { run_id: '"rt_7"', label: '"mesh-a"', stop_reason: 'end_turn' }) + '\n\nOK',
    )
    expect(result.details.last).toMatchObject({ run_id: 'rt_7', runtime_id: 'rt_7', label: 'mesh-a' })
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

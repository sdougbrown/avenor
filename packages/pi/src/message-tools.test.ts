import { afterEach, describe, expect, it, mock } from 'bun:test'
import conformance from '../../../schemas/message_route.conformance.json'
import resultFixture from '../../../schemas/message_route.result.json'
import { messageTool as realMessageTool, renderLast, type MessageProvider } from '@dougbots/avenor-core'
import { makeMessageProvider, startExtensionHarness } from './test-fixtures.js'

const openHarnesses: Array<{
  harness: Awaited<ReturnType<typeof startExtensionHarness>>
  closed: boolean
}> = []

/** Track each started extension so a test that forgets to shut down is caught. */
async function startHarness(options: Parameters<typeof startExtensionHarness>[0] = {}) {
  const harness = await startExtensionHarness(options)
  const record = { harness, closed: false }
  const shutdown = harness.eventHandlers.session_shutdown
  harness.eventHandlers.session_shutdown = async (...args: any[]) => {
    record.closed = true
    return shutdown(...args)
  }
  openHarnesses.push(record)
  return harness
}

afterEach(async () => {
  const leaked = openHarnesses.filter(record => !record.closed)
  for (const record of leaked) await record.harness.eventHandlers.session_shutdown({}, {})
  openHarnesses.length = 0
  if (leaked.length > 0) throw new Error(`${leaked.length} test(s) left the extension poll running`)
})

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
    const harness = await startHarness({})
    const tool = harness.registeredTools.avenor_message
    const { fields, actions } = registered(tool.parameters)
    expect(fields).toEqual(fieldSet(fixture.applicability))
    expect(actions).toEqual(['ask', 'control_reply', 'reply', 'resume', 'tell', 'withdraw'])
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('registers avenor_peers with the contract field set in host mode', async () => {
    const harness = await startHarness({})
    const { fields, actions } = registered(harness.registeredTools.avenor_peers.parameters)
    expect(fields).toEqual(fieldSet(fixture.peers_applicability))
    expect(actions).toEqual(['last', 'list', 'rename'])
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('keeps both tool field sets in child mode', async () => {
    const harness = await startHarness({
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
    })
    expect(registered(harness.registeredTools.avenor_message.parameters).fields)
      .toEqual(fieldSet(fixture.applicability))
    const peers = registered(harness.registeredTools.avenor_peers.parameters)
    expect(peers.fields).toEqual(fieldSet(fixture.peers_applicability))
    expect(peers.actions).toEqual(['last', 'list', 'rename'])
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi host turn producer', () => {
  function recordingProvider(record: ReturnType<typeof mock>): MessageProvider {
    return makeMessageProvider({ recordLastTurn: record as unknown as MessageProvider['recordLastTurn'] })
  }

  function turnEnd(message: unknown) {
    return { type: 'turn_end', turnIndex: 0, message, toolResults: [] }
  }

  it('publishes the host turn on turn_end', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    await harness.eventHandlers.turn_end(turnEnd({
      role: 'assistant', content: [{ type: 'text', text: 'the answer' }], stopReason: 'stop',
    }))
    expect(record).toHaveBeenCalledWith('the answer', 'end_turn', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes an empty or cancelled turn instead of skipping it', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    await harness.eventHandlers.turn_end(turnEnd({ role: 'assistant', content: [], stopReason: 'aborted' }))
    expect(record).toHaveBeenCalledWith('', 'cancelled', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes the truncation flag carried from the unbounded text', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    const long = 'x'.repeat(5000)
    await harness.eventHandlers.turn_end(turnEnd({ role: 'assistant', content: long, stopReason: 'stop' }))
    const [text, stop, truncated] = record.mock.calls[0] as [string, string, boolean]
    expect(text).toHaveLength(4096)
    expect(stop).toBe('end_turn')
    expect(truncated).toBe(true)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes every turn end, including an error turn', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'first attempt', stopReason: 'error' }))
    await harness.eventHandlers.turn_end(turnEnd({ content: 'retry', stopReason: 'stop' }))
    expect(record).toHaveBeenCalledTimes(2)
    expect(record.mock.calls[0]).toEqual(['first attempt', 'error', false])
    expect(record.mock.calls[1]).toEqual(['retry', 'end_turn', false])
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('publishes nothing in child mode', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const harness = await startHarness({
      deps: { hostProviderFor: mock(async () => recordingProvider(record)) },
      extOptions: { subAgentEnv: { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_child', token: 'tok' } },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'child turn', stopReason: 'stop' }))
    expect(record).not.toHaveBeenCalled()
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('logs a throwing session_last_set without rejecting the handler', async () => {
    const record = mock(async () => {
      throw new Error('broker down')
    })
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    await expect(harness.eventHandlers.turn_end(turnEnd({ content: 'turn', stopReason: 'stop' }))).resolves.toBeUndefined()
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
    const harness = await startHarness({
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
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => provider) } })
    const result = await harness.registeredTools.avenor_peers.execute('t1', { action: 'last', target: 'mesh-a' })
    expect(result.content[0].text).toBe(
      renderLast('header', { run_id: '"rt_7"', label: '"mesh-a"', stop_reason: 'end_turn' }) + '\n\nOK',
    )
    expect(result.details.last).toMatchObject({ run_id: 'rt_7', runtime_id: 'rt_7', label: 'mesh-a' })
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi surface result objects', () => {
  it('returns the shared result objects for every message and peers action', async () => {
    const expected = resultFixture as unknown as {
      message: Record<string, Record<string, unknown>>
      peers: Record<string, Record<string, unknown>>
    }
    const liveAt = () => makeMessageProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: true, resumable: false, status: 'running' }),
      tell: async () => ({ outcome: 'sent', messageId: 'm-1' }),
    })
    const resumableAt = () => makeMessageProvider({
      resolve: async () => ({ runId: 'mesh-a', label: 'mesh-a', match: 'unique' }),
      state: async () => ({ live: false, resumable: true, status: 'done' }),
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-2',
        runtimeId: 'rt-2',
        label: 'again',
        sentinelFile: '/runs/run-2/sentinel.done',
        onEvent: '/runs/run-2/events.log',
      }),
    })

    let provider = liveAt()
    const harness = await startHarness({
      deps: { messageTool: realMessageTool, hostProviderFor: mock(async () => provider) },
    })

    const tell = await harness.registeredTools.avenor_message.execute(
      't1', { action: 'tell', target: 'mesh-a', message: 'hi' },
    )
    expect(tell.details).toEqual(expected.message.tell)
    expect(tell.content[0].text).toBe(expected.message.tell.text)

    provider = resumableAt()
    const resume = await harness.registeredTools.avenor_message.execute(
      't2', { action: 'resume', target: 'mesh-a', message: 'again' },
    )
    expect(resume.details).toEqual(expected.message.resume)

    provider = makeMessageProvider()
    const reply = await harness.registeredTools.avenor_message.execute(
      't3', { action: 'reply', reply_to: 'a-1', message: 'pong' },
    )
    expect(reply.details).toEqual(expected.message.reply_edge_absent)

    provider = makeMessageProvider({
      peers: async () => ({
        peers: [
          { run_id: 'rt_2', label: 'mesh-a', kind: 'agent', status: 'running' },
          { run_id: 'rt_3' },
        ],
      }),
      rename: async () => ({ outcome: 'renamed', runId: 'rt_2', label: 'mesh-renamed', previousLabel: 'mesh-a' }),
      resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }),
      last: async () => ({ outcome: 'sent', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' }),
    })
    const list = await harness.registeredTools.avenor_peers.execute('p1', { action: 'list' })
    expect(list.details).toEqual(expected.peers.list)
    const rename = await harness.registeredTools.avenor_peers.execute('p2', { action: 'rename', name: 'mesh-renamed' })
    expect(rename.details).toEqual(expected.peers.rename)
    const last = await harness.registeredTools.avenor_peers.execute('p3', { action: 'last', target: 'mesh-a' })
    expect(last.details).toEqual(expected.peers.last)

    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi default host provider wiring', () => {
  it('drives the real bound provider when no hostProviderFor is injected', async () => {
    const send = mock(async (params: { message_id: string }) => ({ outcome: 'sent', message_id: params.message_id }))
    const client = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      messageSend: send,
      messagePeers: mock(async () => ({
        peers: [{ run_id: 'supervisor', session: { run_id: 'supervisor', status: 'running' } }],
      })),
    }
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' })) as any,
        messageTool: realMessageTool,
      },
    })
    const result = await harness.registeredTools.avenor_message.execute('t1', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })
    expect(result.details.outcome).toBe('sent')
    expect(send).toHaveBeenCalledWith(expect.objectContaining({
      to_run_id: 'supervisor',
      generation: 'gen-1',
      no_reply: true,
    }))
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

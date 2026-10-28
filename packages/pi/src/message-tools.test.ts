import { afterEach, describe, expect, it, mock } from 'bun:test'
import conformance from '../../../schemas/message_route.conformance.json'
import resultFixture from '../../../schemas/message_route.result.json'
import { ClosedSocketError, messageTool as realMessageTool, renderLast, RpcError, type MessageProvider } from '@dougbots/avenor-core'
import { CHANNEL_LAST_TURN_ERROR } from './index.js'
import { makeMessageProvider, startExtensionHarness, waitFor } from './test-fixtures.js'

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
  applicability: Record<string, { permitted: string[], types: Record<string, string> }>
  peers_applicability: Record<string, { permitted: string[], types: Record<string, string> }>
}

function fieldSet(table: Record<string, { permitted: string[] }>): string[] {
  const names = new Set<string>()
  for (const entry of Object.values(table)) {
    for (const name of entry.permitted) names.add(name)
  }
  return [...names].sort()
}

/** The JSON type the contract declares per property name, across all actions. */
function contractTypes(table: Record<string, { types: Record<string, string> }>): Record<string, string> {
  const types: Record<string, string> = {}
  for (const entry of Object.values(table)) {
    for (const [name, type] of Object.entries(entry.types)) {
      if (types[name] !== undefined && types[name] !== type) throw new Error(`contract declares ${name} as both ${types[name]} and ${type}`)
      types[name] = type
    }
  }
  return types
}

/**
 * The JSON type a hand-written parameter schema declares, or null when it
 * constrains none (an unknown payload admits whatever type the contract names).
 */
function declaredType(schema: unknown): string | null {
  const json = schema as { type?: unknown, anyOf?: Array<{ const?: unknown }> }
  if (typeof json.type === 'string') return json.type
  const constants = json.anyOf ?? []
  if (constants.length > 0 && constants.every(entry => typeof entry.const === 'string')) return 'string'
  return null
}

/** Every shared property whose declared JSON type disagrees with the contract. */
function typeMismatches(
  toolName: string,
  schema: unknown,
  table: Record<string, { types: Record<string, string> }>,
): string[] {
  const properties = (schema as { properties: Record<string, unknown> }).properties
  const mismatched: string[] = []
  for (const [name, want] of Object.entries(contractTypes(table))) {
    const declared = declaredType(properties[name])
    if (declared !== null && declared !== want) mismatched.push(`${toolName}.${name}: ${declared}, contract ${want}`)
  }
  return mismatched
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

  it('keeps the contract JSON type for every shared parameter', async () => {
    const harness = await startHarness({})
    expect([
      ...typeMismatches('avenor_message', harness.registeredTools.avenor_message.parameters, fixture.applicability),
      ...typeMismatches('avenor_peers', harness.registeredTools.avenor_peers.parameters, fixture.peers_applicability),
    ]).toEqual([])
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

  it('opens no host provider when the session has no supervisor', async () => {
    const getSupervisorClient = mock(async () => {
      throw new Error('a chat-only turn must not start a supervisor')
    })
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        Supervisor: class {
          static currentInstance() {
            return null
          }
        } as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'chat only', stopReason: 'stop' }))
    expect(getSupervisorClient).not.toHaveBeenCalled()
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('records the turn through the bound provider when a supervisor exists', async () => {
    const sessionLastSet = mock(async () => ({}))
    const supervisorInstance = { supervisorId: 'sup', getClient: () => ({}) }
    const client = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      sessionLastSet,
    }
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' }))
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        Supervisor: class {
          static currentInstance() {
            return supervisorInstance
          }
        } as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'the answer', stopReason: 'stop' }))
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    expect(sessionLastSet).toHaveBeenCalledWith('supervisor', 'the answer', 'end_turn', false)
    expect(harness.notify).not.toHaveBeenCalled()
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

  it('reports a closed control socket once through the session UI and keeps attempting', async () => {
    const record = mock(async () => {
      throw new ClosedSocketError('control socket is closed')
    })
    const errorLog = mock((_message?: unknown, ..._rest: unknown[]) => {})
    const originalError = console.error
    console.error = errorLog
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    try {
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
      expect(record).toHaveBeenCalledTimes(1)
      expect(harness.notify).toHaveBeenCalledTimes(1)
      expect(String(harness.notify.mock.calls[0]?.[0])).toContain('session_last_set failed: control socket is closed')
      expect(errorLog).not.toHaveBeenCalled()
      // The next turn keeps attempting so a healthy supervisor is picked up,
      // but the still-closed connection is not reported again.
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
      expect(record).toHaveBeenCalledTimes(2)
      expect(harness.notify).toHaveBeenCalledTimes(1)
      // A new session re-arms the report latch.
      await harness.eventHandlers.session_start({}, harness.ctx)
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn three', stopReason: 'stop' }))
      expect(record).toHaveBeenCalledTimes(3)
      expect(harness.notify).toHaveBeenCalledTimes(2)
    } finally {
      console.error = originalError
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('reports a transient session_last_set failure once per episode', async () => {
    const record = mock(async () => {
      throw new Error('broker down')
    })
    const harness = await startHarness({ deps: { hostProviderFor: mock(async () => recordingProvider(record)) } })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
    // The attempt retries on every turn; the report latches until a record
    // succeeds, so a persistently failing broker raises one toast, not one per
    // turn.
    expect(record).toHaveBeenCalledTimes(2)
    expect(harness.notify).toHaveBeenCalledTimes(1)
    // Suppressed repeats stay observable: every failure is emitted on the
    // last-turn error channel, even when the toast is latched, with the
    // failure text as the payload's error.
    const lastTurnErrors = harness.emitted.filter(([channel]) => channel === CHANNEL_LAST_TURN_ERROR)
    expect(lastTurnErrors).toHaveLength(2)
    const [, payload] = lastTurnErrors[1] as [{}, { message: string; error: string }]
    expect(payload.message).toBe('session_last_set failed')
    expect(payload.error).toBe('broker down')
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('recovers last-turn recording after a generation mismatch without session_start', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const closedRecord = mock(async () => { throw new ClosedSocketError('control socket is closed') })
    let provider = makeMessageProvider({ recordLastTurn: closedRecord })
    const hostProviderFor = mock(async () => provider)
    let receiveCount = 0
    const brokerReceiveTool = mock(async (args: { generation?: string }) => {
      receiveCount++
      if (args.generation === 'gen-1') {
        throw new RpcError(-32000, 'messaging: generation mismatch', {
          code: 'generation-mismatch',
          outcome: 'not-sent',
          wire_code: 'generation-mismatch',
        })
      }
      return { asks: [], generation: receiveCount === 1 ? 'gen-1' : 'gen-2' }
    })
    const errorLog = mock((_message?: unknown, ..._rest: unknown[]) => {})
    const originalError = console.error
    const harness = await startHarness({
      deps: { hostProviderFor, brokerReceiveTool },
      extOptions: { subAgentEnv: null, pollIntervalMs: 5 },
    })
    console.error = errorLog
    try {
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
      expect(closedRecord).toHaveBeenCalledTimes(2)
      expect(harness.notify).toHaveBeenCalledTimes(1)
      expect(errorLog).not.toHaveBeenCalled()
      await waitFor(() => receiveCount >= 3)
      // The wholesale rebind cleared the latch: the same failure reports again
      // on the rebound generation instead of staying suppressed.
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn three', stopReason: 'stop' }))
      expect(closedRecord).toHaveBeenCalledTimes(3)
      expect(harness.notify).toHaveBeenCalledTimes(2)
      // A healthy provider publishes again without a session restart.
      provider = makeMessageProvider({ recordLastTurn: record })
      await harness.eventHandlers.turn_end(turnEnd({ content: 'turn four', stopReason: 'stop' }))
      expect(record).toHaveBeenCalledWith('turn four', 'end_turn', false)
    } finally {
      console.error = originalError
      await harness.eventHandlers.session_shutdown({}, {})
    }
  })

  it('redials a closed default binding and resumes publication without session_start', async () => {
    const record = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const closedSet = mock(async (_text: string, _stop: string, _truncated: boolean) => {
      throw new ClosedSocketError('control socket is closed')
    })
    const client = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      sessionLastSet: closedSet as unknown as (_text: string, _stop: string, _truncated: boolean) => Promise<void>,
    }
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' }))
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(1)
    // The failed binding was evicted, so the next turn redials instead of
    // reusing the closed client, and the still-closed connection is not
    // reported again.
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
    expect(closedSet).toHaveBeenCalledTimes(2)
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    expect(harness.notify).toHaveBeenCalledTimes(1)
    // A healthy connection publishes again and re-arms the report latch.
    client.sessionLastSet = record
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn three', stopReason: 'stop' }))
    expect(getSupervisorClient).toHaveBeenCalledTimes(3)
    expect(record).toHaveBeenCalledWith('supervisor', 'turn three', 'end_turn', false)
    // A later failure on the new connection is a new episode and is reported.
    client.sessionLastSet = closedSet
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn four', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('keeps a concurrently re-dialled binding when the failed record evicts', async () => {
    // Umpire round 2, finding 3: the failing record's eviction must not
    // discard a binding that a concurrent tool call cached. The failing record
    // first drops the stale binding and runs a host tool, which redials and
    // caches a fresh binding; the catch must not evict that one.
    const record2 = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const client1 = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      sessionLastSet: mock(async () => {
        await harness.registeredTools.avenor_shutdown.execute('t1', {})
        await harness.registeredTools.avenor_peers.execute('t2', { action: 'list' })
        throw new ClosedSocketError('control socket is closed')
      }),
    }
    const client2 = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-2' })),
      messagePeers: mock(async () => ({ peers: [], generation: 'gen-2' })),
      sessionLastSet: record2,
    }
    let dial = 0
    const getSupervisorClient = mock(async () => {
      dial++
      return { client: dial === 1 ? client1 : client2, isSingleton: true, sup: null, supervisorId: 'sup' }
    })
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(1)
    // The fresh binding survives the eviction, so the next turn records
    // without another dial.
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    expect(record2).toHaveBeenCalledWith('supervisor', 'turn two', 'end_turn', false)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('reports a replacement supervisor its own first failure without session_start', async () => {
    // Umpire round 1, finding 1: supervisor A's socket dies (reported once),
    // avenor_shutdown stops A and a replacement B is dialled, and B fails
    // before any successful record. B's failure is its own episode: the stale
    // latch from A must not suppress it.
    const closedSet = mock(async (_text: string, _stop: string, _truncated: boolean) => {
      throw new ClosedSocketError('control socket is closed')
    })
    const client = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      sessionLastSet: closedSet as unknown as (_text: string, _stop: string, _truncated: boolean) => Promise<void>,
    }
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' }))
    const instanceA = { supervisorId: '/tmp/a.sock', getClient: () => ({}) }
    const instanceB = { supervisorId: '/tmp/b.sock', getClient: () => ({}) }
    let current = instanceA
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        Supervisor: class {
          static currentInstance() {
            return current
          }
        } as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(1)
    await harness.registeredTools.avenor_shutdown.execute('s1', {})
    current = instanceB
    // B fails before any successful record: the report must be B's own.
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    expect(harness.notify).toHaveBeenCalledTimes(2)
    // B is still dead: the latch suppresses repeats for the same instance.
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn three', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('reports an unhealthy supervisor once and recovers when it is healthy again', async () => {
    // The guard branch's eviction must be exercised against a pre-existing
    // binding: the binding is established and records, the instance's
    // getClient() starts throwing (report once, no dial), and the recovery
    // turn redials and records without an extra failed turn.
    const client1SessionLastSet = mock(async () => {})
    const client2SessionLastSet = mock(async (_text: string, _stop: string, _truncated: boolean) => {})
    const client1 = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      sessionLastSet: client1SessionLastSet as unknown as (_text: string, _stop: string, _truncated: boolean) => Promise<void>,
    }
    const client2 = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-2' })),
      sessionLastSet: client2SessionLastSet as unknown as (_text: string, _stop: string, _truncated: boolean) => Promise<void>,
    }
    let dial = 0
    const getSupervisorClient = mock(async () => {
      dial++
      return { client: dial === 1 ? client1 : client2, isSingleton: true, sup: null, supervisorId: 'sup' }
    })
    const healthyInstance = { supervisorId: 'sup', getClient: () => ({}) }
    const deadInstance = {
      supervisorId: 'sup',
      getClient: () => {
        throw new Error('avenor supervisor connection closed')
      },
    }
    let current = healthyInstance
    const harness = await startHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        Supervisor: class {
          static currentInstance() {
            return current
          }
        } as any,
      },
    })
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn one', stopReason: 'stop' }))
    expect(client1SessionLastSet).toHaveBeenCalledTimes(1)
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    expect(harness.notify).not.toHaveBeenCalled()
    // The connection dies: the guard reports once and evicts without dialing.
    current = deadInstance
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn two', stopReason: 'stop' }))
    expect(harness.notify).toHaveBeenCalledTimes(1)
    expect(String(harness.notify.mock.calls[0]?.[0])).toContain('session_last_set failed: avenor supervisor connection closed')
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    expect(client1SessionLastSet).toHaveBeenCalledTimes(1)
    // The supervisor recovers: the next turn redials and records, with no
    // redundant failed record on the stale binding.
    current = healthyInstance
    await harness.eventHandlers.turn_end(turnEnd({ content: 'turn three', stopReason: 'stop' }))
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    expect(client2SessionLastSet).toHaveBeenCalledWith('supervisor', 'turn three', 'end_turn', false)
    expect(harness.notify).toHaveBeenCalledTimes(1)
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

describe('Pi resumed-run tracking', () => {
  it('tracks the resumed run on its supervisor and starts polling', async () => {
    const statusTool = mock(async (args: { runId?: string }) => ({
      run_id: args.runId ?? 'run-9',
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
  /** A control client fake plus the close spy the cache may or may not call. */
  function fakeClient() {
    const close = mock(() => {})
    const client = {
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      messageSend: mock(async (params: { message_id: string }) => ({ outcome: 'sent', message_id: params.message_id })),
      messagePeers: mock(async () => ({
        peers: [{ run_id: 'supervisor', session: { run_id: 'supervisor', status: 'running' } }],
      })),
      close,
    }
    return { client, close }
  }

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

  it('closes the owner socket it dialled and redials after the session shuts down', async () => {
    const closes = [mock(() => {}), mock(() => {})]
    const clients = closes.map(close => ({
      messagePreflight: mock(async () => ({ protocol: 1, generation: 'gen-1' })),
      messageSend: mock(async (params: { message_id: string }) => ({ outcome: 'sent', message_id: params.message_id })),
      messagePeers: mock(async () => ({
        peers: [{ run_id: 'supervisor', session: { run_id: 'supervisor', status: 'running' } }],
      })),
      close,
    }))
    const getSupervisorClient = mock(async () => ({
      client: clients.shift(),
      isSingleton: false,
      sup: null,
      supervisorId: '/tmp/other.sock',
    }))
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        messageTool: realMessageTool,
      },
    })
    const tell = () => harness.registeredTools.avenor_message.execute('t', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })

    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    await harness.eventHandlers.session_shutdown({}, {})
    expect(closes[0]).toHaveBeenCalledTimes(1)

    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
    expect(closes[1]).toHaveBeenCalledTimes(1)
  })

  it('leaves the shared singleton client open and rebinds it after the session shuts down', async () => {
    const { client, close } = fakeClient()
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' }))
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        messageTool: realMessageTool,
      },
    })
    const tell = () => harness.registeredTools.avenor_message.execute('t', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })

    await tell()
    await harness.eventHandlers.session_shutdown({}, {})
    expect(close).not.toHaveBeenCalled()

    // The binding is gone even though its client is not closed, so the next
    // call preflights again on the same live client.
    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    expect(client.messageSend).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
    expect(close).not.toHaveBeenCalled()
  })

  it('evicts the singleton binding when avenor_shutdown stops the singleton', async () => {
    const { client, close } = fakeClient()
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: 'sup' }))
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        messageTool: realMessageTool,
      },
    })
    const tell = () => harness.registeredTools.avenor_message.execute('t', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })

    await tell()
    await harness.registeredTools.avenor_shutdown.execute('s', {})
    expect(close).not.toHaveBeenCalled()

    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('evicts both bindings when avenor_shutdown names the singleton socket', async () => {
    const SINGLETON = '/tmp/singleton.sock'
    const { client, close } = fakeClient()
    const getSupervisorClient = mock(async () => ({ client, isSingleton: true, sup: null, supervisorId: SINGLETON }))
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        messageTool: realMessageTool,
        Supervisor: class {
          static currentInstance() {
            return { supervisorId: SINGLETON }
          }

          static isCurrentInstance(supervisorId: string) {
            return supervisorId === SINGLETON
          }
        } as any,
      },
    })
    const tell = () => harness.registeredTools.avenor_message.execute('t', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })

    // Only the default binding exists, and it holds the process-shared
    // singleton client the shutdown names by socket.
    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    await harness.registeredTools.avenor_shutdown.execute('s', { supervisor_id: SINGLETON })
    expect(close).not.toHaveBeenCalled()

    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
  })

  it('keeps the default binding when avenor_shutdown targets another supervisor', async () => {
    const OTHER = '/tmp/other.sock'
    const { client, close } = fakeClient()
    const other = fakeClient()
    const getSupervisorClient = mock(async (supervisorId?: string) => supervisorId === OTHER
      ? { client: other.client, isSingleton: false, sup: null, supervisorId: OTHER }
      : { client, isSingleton: true, sup: null, supervisorId: 'sup' })
    const harness = await startExtensionHarness({
      deps: {
        hostProviderFor: undefined,
        getSupervisorClient: getSupervisorClient as any,
        messageTool: realMessageTool,
      },
    })
    const tell = () => harness.registeredTools.avenor_message.execute('t', {
      action: 'tell',
      target: 'self',
      message: 'hi',
    })

    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    await harness.registeredTools.avenor_shutdown.execute('s', { supervisor_id: OTHER })
    expect(close).not.toHaveBeenCalled()

    // The default binding survived, so the next call reuses it instead of
    // dialling again.
    await tell()
    expect(getSupervisorClient).toHaveBeenCalledTimes(1)
    expect(client.messageSend).toHaveBeenCalledTimes(2)
    await harness.eventHandlers.session_shutdown({}, {})
  })
})

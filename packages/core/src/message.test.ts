import { describe, expect, it } from 'bun:test'

import messageInputSchema from '../../../schemas/message.input.json'
import conformanceFixture from '../../../schemas/message_route.conformance.json'
import outcomeSchema from '../../../schemas/message.outcome.json'
import peersInputSchema from '../../../schemas/peers.input.json'
import resultFixture from '../../../schemas/message_route.result.json'
import {
  assertMessageInput,
  assertPeersInput,
  executeMessage,
  executePeers,
  normalizeRouteRules,
  peersDetails,
  resultOutput,
  ToolError,
} from './message.js'
import {
  MESSAGE_OUTCOMES,
  normalizeTurnStopReason,
  quote,
  renderLast,
  renderOutcome,
  type MessageOutcome,
} from './messaging-text.js'
import type { MessageProvider, PeerRecord, ProviderResult } from './tools/message.js'

const fixture = conformanceFixture as unknown as {
  applicability: Record<string, unknown>
  cases: FixtureCase[]
  peers_cases: FixtureCase[]
}

interface FixtureCase {
  name: string
  input: unknown
  conditions?: Record<string, unknown>
  provider?: Record<string, unknown>
  schema?: boolean
  expect: {
    code?: string
    facts?: Record<string, string>
    message: string
    outcome?: string
  }
}

const messageId = 'av1.test.1'

type JsonObject = globalThis.Record<string, unknown>

function object(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined
}

function objects(value: unknown): JsonObject[] {
  return Array.isArray(value)
    ? value.map(object).filter((item): item is JsonObject => item !== undefined)
    : []
}

function stringOf(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

/** One provider driven by a conformance case, recording what the router sent it. */
function fakeProvider(testCase: FixtureCase) {
  const conditions = testCase.conditions ?? {}
  const spec = testCase.provider ?? {}
  const resolved = object(spec.resolved)
  const state = object(spec.state)
  const calls: {
    resolveRef?: string
    tell?: { targetRunId: string; message: string }
    ask?: { targetRunId: string; message: string; timeoutMs?: number }
    reply?: { inboundAskId: string; message: string }
    withdraw?: { messageId: string }
    resume?: { target: string; message: string; label?: string; idempotencyKey?: string }
    rename?: { sessionId?: string; name: string }
    last?: string
    controlReply?: { target: string; payload: unknown }
    peers?: boolean
  } = {}
  const claimed = new Map<string, string>()
  const claim = object(spec.claim)
  if (claim) claimed.set(stringOf(claim.message_id), stringOf(claim.asker_run_id))

  const provider: MessageProvider & { calls: typeof calls; claimed: Map<string, string> } = {
    calls,
    claimed,
    caller: () => ({
      kind: conditions.callerKind === 'owner'
        ? 'owner'
        : conditions.callerKind === 'broker'
          ? 'broker'
          : ('' as 'owner'),
      runId: conditions.callerKind === 'owner' ? 'supervisor' : 'run-caller',
    }),
    surface: () => stringOf(conditions.surface) as never,
    role: () => 'agent',
    async resolve(ref: string) {
      calls.resolveRef = ref
      return {
        runId: stringOf(resolved?.run_id),
        publicRunId: stringOf(resolved?.public_run_id),
        label: stringOf(resolved?.label),
        candidates: Array.isArray(resolved?.candidates) ? (resolved?.candidates as string[]) : [],
        match: (stringOf(conditions.targetMatch) || 'unique') as 'unique' | 'ambiguous' | 'none',
      }
    },
    async state() {
      return {
        live: conditions.targetLive === true,
        resumable: conditions.targetResumable === true,
        status: stringOf(state?.status),
      }
    },
    async tell(targetRunId: string, message: string): Promise<ProviderResult> {
      calls.tell = { targetRunId, message }
      if (spec.fence_full === true) return { outcome: 'not-sent', messageId }
      return { outcome: stringOf(spec.outcome) as MessageOutcome, messageId }
    },
    async ask(targetRunId: string, message: string, timeoutMs?: number): Promise<ProviderResult> {
      calls.ask = { targetRunId, message, timeoutMs }
      const outcome = stringOf(spec.outcome)
      const result: ProviderResult = { outcome: outcome as MessageOutcome, messageId }
      if (outcome === 'replied') result.reply = stringOf(spec.reply)
      return result
    },
    async reply(inboundAskId: string, message: string): Promise<ProviderResult> {
      calls.reply = { inboundAskId, message }
      return { outcome: stringOf(spec.outcome) as MessageOutcome, messageId: inboundAskId }
    },
    async withdraw(id: string): Promise<ProviderResult> {
      calls.withdraw = { messageId: id }
      return { outcome: stringOf(spec.outcome) as MessageOutcome }
    },
    async resume(req): Promise<ProviderResult> {
      calls.resume = req
      const resumed = object(spec.resume) ?? {}
      return {
        outcome: 'resumed',
        runId: stringOf(resumed.run_id),
        runtimeId: stringOf(resumed.runtime_id),
        label: stringOf(resumed.label),
        sentinelFile: stringOf(resumed.sentinel_file),
        onEvent: stringOf(resumed.on_event),
      }
    },
    async rename(req): Promise<ProviderResult> {
      calls.rename = req
      const renamed = object(spec.rename)
      if (renamed) {
        return {
          outcome: 'renamed',
          runId: stringOf(renamed.run_id),
          label: stringOf(renamed.label),
          previousLabel: stringOf(renamed.previous_label),
        }
      }
      const failure = object(spec.rename_error) ?? {}
      throw new ToolError(stringOf(failure.code), stringOf(failure.message))
    },
    async last(runId: string): Promise<ProviderResult> {
      calls.last = runId
      const last = object(spec.last)
      if (!last) throw new Error(stringOf(spec.last_error))
      return {
        outcome: 'sent',
        label: stringOf(last.label),
        hasRecord: last.has_record === true,
        finalOutput: stringOf(last.final_output),
        stopReason: stringOf(last.stop_reason),
        truncated: last.truncated === true,
      }
    },
    async peers() {
      calls.peers = true
      const peers: PeerRecord[] = objects(spec.peers).map(peer => {
        const record: PeerRecord = {
          run_id: stringOf(peer.run_id) || stringOf(peer.peer_run_id),
        }
        for (const key of ['label', 'kind', 'status', 'backend', 'model', 'dir'] as const) {
          if (typeof peer[key] === 'string') record[key] = peer[key] as string
        }
        return record
      })
      const detail = stringOf(spec.peers_error)
      return { peers, ...(detail ? { detail } : {}) }
    },
  }

  if (claim) {
    provider.claimAsk = (id: string) => claimed.get(id)
    provider.restoreAsk = (id: string, asker: string) => {
      claimed.set(id, asker)
    }
  }
  if (object(spec.control_reply)) {
    provider.controlReply = async (target: string, payload: unknown) => {
      calls.controlReply = { target, payload }
      return { outcome: stringOf(spec.outcome) as MessageOutcome }
    }
  }
  return provider
}

function substituteMessageId(text: string, id: string): string {
  return text.split('{message_id}').join(id)
}

function assertNoWrite(provider: ReturnType<typeof fakeProvider>): void {
  const calls = provider.calls
  if (
    calls.tell ||
    calls.ask ||
    calls.reply ||
    calls.withdraw ||
    calls.resume ||
    calls.rename ||
    calls.last ||
    calls.controlReply ||
    calls.peers
  ) {
    throw new Error('a rejected request reached the provider')
  }
}

describe('message conformance', () => {
  it('runs every shared message case', async () => {
    expect(fixture.cases.length).toBeGreaterThan(0)
    for (const testCase of fixture.cases) {
      const provider = fakeProvider(testCase)
      const input = testCase.input as { action?: string }
      const expected = testCase.expect

      if (expected.code) {
        let error: unknown
        try {
          await executeMessage(provider, testCase.input)
        } catch (caught) {
          error = caught
        }
        expect(error, testCase.name).toBeInstanceOf(ToolError)
        const toolError = error as ToolError
        expect(toolError.code, testCase.name).toBe(expected.code)
        expect(toolError.message, testCase.name).toBe(expected.message)
        expect(toolError.facts, testCase.name).toEqual(expected.facts ?? {})
        assertNoWrite(provider)
        continue
      }

      const result = await executeMessage(provider, testCase.input)
      expect(result.text, `${testCase.name}: a successful call produced no text`).not.toBe('')
      expect(result.outcome, testCase.name).toBe(expected.outcome ?? '')
      expect(substituteMessageId(result.text ?? '', result.messageId ?? ''), testCase.name).toBe(
        substituteMessageId(expected.message, result.messageId ?? ''),
      )
      expect(result.action, testCase.name).toBe(input.action)
      assertMessageRequest(testCase, provider, object(testCase.provider?.request))
    }
  })

  it('runs every shared peers case', async () => {
    expect(fixture.peers_cases.length).toBeGreaterThan(0)
    for (const testCase of fixture.peers_cases) {
      const provider = fakeProvider(testCase)
      const expected = testCase.expect

      if (expected.code) {
        let error: unknown
        try {
          await executePeers(provider, testCase.input)
        } catch (caught) {
          error = caught
        }
        expect(error, testCase.name).toBeInstanceOf(ToolError)
        const toolError = error as ToolError
        expect(toolError.code, testCase.name).toBe(expected.code)
        expect(toolError.message, testCase.name).toBe(expected.message)
        if (expected.code === 'invalid_request' || expected.code === 'invalid_action') {
          if (provider.calls.peers || provider.calls.rename || provider.calls.last || provider.calls.resolveRef) {
            throw new Error(`${testCase.name}: a rejected request reached the provider`)
          }
        }
        continue
      }

      const result = await executePeers(provider, testCase.input)
      expect(result.outcome ?? '', testCase.name).toBe(expected.outcome ?? '')
      expect(result.text, testCase.name).toBe(expected.message)
      assertPeersResult(testCase, provider, result)
    }
  })
})

function assertMessageRequest(
  testCase: FixtureCase,
  provider: ReturnType<typeof fakeProvider>,
  request: JsonObject | undefined,
): void {
  const spec = testCase.provider ?? {}
  const resolved = object(spec.resolved) ?? {}
  const ref = stringOf(spec.resolve_ref)
  if (ref && provider.calls.resolveRef !== ref) {
    throw new Error(`${testCase.name}: resolved ${provider.calls.resolveRef}, want ${ref}`)
  }
  const input = testCase.input as { action?: string }
  switch (input.action) {
    case 'tell':
    case 'ask': {
      const call = input.action === 'tell' ? provider.calls.tell : provider.calls.ask
      if (!call) throw new Error(`${testCase.name}: no ${input.action} call`)
      expect(call.targetRunId, testCase.name).toBe(stringOf(resolved.run_id))
      if (request?.message !== undefined) {
        expect(call.message, testCase.name).toBe(stringOf(request.message))
      }
      if (input.action === 'ask' && request?.timeout_ms !== undefined) {
        expect(provider.calls.ask?.timeoutMs, testCase.name).toBe(request.timeout_ms as number)
      }
      break
    }
    case 'reply':
      if (request?.message_id !== undefined) {
        expect(provider.calls.reply?.inboundAskId, testCase.name).toBe(stringOf(request.message_id))
      }
      if (request?.message !== undefined) {
        expect(provider.calls.reply?.message, testCase.name).toBe(stringOf(request.message))
      }
      break
    case 'withdraw':
      if (request?.message_id !== undefined) {
        expect(provider.calls.withdraw?.messageId, testCase.name).toBe(stringOf(request.message_id))
      }
      break
    case 'control_reply': {
      const expected = object(spec.control_reply) ?? {}
      const controlRequest = object(expected.request) ?? {}
      if (controlRequest.target !== undefined) {
        expect(provider.calls.controlReply?.target, testCase.name).toBe(stringOf(controlRequest.target))
      }
      expect(provider.calls.controlReply?.payload, testCase.name).toEqual(expected.payload ?? {})
      break
    }
    case 'resume': {
      if (!provider.calls.resume) throw new Error(`${testCase.name}: no resume call`)
      if (request?.target !== undefined) {
        expect(provider.calls.resume.target, testCase.name).toBe(stringOf(request.target))
      }
      if (request?.message !== undefined) {
        expect(provider.calls.resume.message, testCase.name).toBe(stringOf(request.message))
      }
      if (request?.label !== undefined) {
        expect(provider.calls.resume.label, testCase.name).toBe(stringOf(request.label))
      }
      if (request?.idempotency_key !== undefined) {
        expect(provider.calls.resume.idempotencyKey, testCase.name).toBe(stringOf(request.idempotency_key))
      }
      break
    }
  }
}

function assertPeersResult(
  testCase: FixtureCase,
  provider: ReturnType<typeof fakeProvider>,
  result: Awaited<ReturnType<typeof executePeers>>,
): void {
  const spec = testCase.provider ?? {}
  const ref = stringOf(spec.resolve_ref)
  if (ref && provider.calls.resolveRef !== ref) {
    throw new Error(`${testCase.name}: resolved ${provider.calls.resolveRef}, want ${ref}`)
  }
  const peers = objects(spec.peers)
  if (peers.length > 0) {
    expect(result.peers.length, testCase.name).toBe(peers.length)
    peers.forEach((peer, index) => {
      const record = result.peers[index]
      expect(record.run_id, testCase.name).toBe(
        stringOf(peer.run_id) || stringOf(peer.peer_run_id),
      )
      for (const key of ['label', 'kind', 'status', 'backend', 'model', 'dir'] as const) {
        expect(record[key], `${testCase.name}: ${key}`).toBe(stringOf(peer[key]) || undefined)
      }
    })
  }
  const renamed = object(spec.rename)
  if (renamed) {
    expect(result.renamed, testCase.name).toEqual({
      run_id: stringOf(renamed.run_id),
      label: stringOf(renamed.label),
      previous_label: stringOf(renamed.previous_label),
    })
  }
  const request = object(spec.request)
  if (request) {
    if (request.session_id !== undefined) {
      expect(provider.calls.rename?.sessionId ?? '', testCase.name).toBe(stringOf(request.session_id))
    }
    if (request.name !== undefined) {
      expect(provider.calls.rename?.name, testCase.name).toBe(stringOf(request.name))
    }
    if (request.run_id !== undefined) {
      expect(provider.calls.last, testCase.name).toBe(stringOf(request.run_id))
    }
  }
  const last = object(spec.last)
  if (last) {
    const resolution = object(spec.resolved) ?? {}
    const target = (testCase.input as { target?: string }).target
    const wantLabel = stringOf(last.label) || stringOf(resolution.label) || target || ''
    expect(result.last?.label, testCase.name).toBe(wantLabel)
    expect(result.last?.final_output, testCase.name).toBe(stringOf(last.final_output))
    expect(result.last?.stop_reason, testCase.name).toBe(stringOf(last.stop_reason))
    expect(result.last?.final_output_truncated, testCase.name).toBe(last.truncated === true)
    expect(result.last?.runtime_id, testCase.name).toBe(stringOf(resolution.run_id))
    expect(result.last?.run_id, testCase.name).toBe(
      stringOf(resolution.public_run_id) || stringOf(resolution.run_id),
    )
    expect(provider.calls.last, testCase.name).toBe(stringOf(resolution.run_id))
  }
}

describe('message admission', () => {
  const actions = Object.keys(fixture.applicability)

  it('declares a table entry for every schema variant', () => {
    const variants = (messageInputSchema as { oneOf: Array<{ properties: Record<string, unknown> }> }).oneOf
    const schemaActions = variants.map(variant => (variant.properties.action as { const: string }).const).sort()
    expect(schemaActions).toEqual([...actions].sort())
  })

  it('validates every action against its permitted and required sets', () => {
    expect(actions.length).toBe(6)
    for (const action of actions) {
      const missing = () => assertMessageInput({ action })
      expect(missing, action).toThrow(ToolError)
      expect(missing, action).toThrow(`invalid input for action "${action}"`)
      expect(() => assertMessageInput({ action, bogus: 1 }), action).toThrow('unknown property "bogus"')
    }
  })

  it('accepts the timeout only as an integer inside the shared wait cap', () => {
    expect(() => assertMessageInput({ action: 'ask', target: 'rt_7', message: 'hi', timeout_ms: 1000 })).not.toThrow()
    expect(() => assertMessageInput({ action: 'ask', target: 'rt_7', message: 'hi', timeout_ms: 600000 })).not.toThrow()
    for (const timeout of [1.5, Number.NaN, Number.POSITIVE_INFINITY, -1, 600001]) {
      expect(() => assertMessageInput({ action: 'ask', target: 'rt_7', message: 'hi', timeout_ms: timeout })).toThrow(ToolError)
    }
  })

  it('rejects an integral timeout outside int64 as a type error, like the Go router', () => {
    expect(() => assertMessageInput({ action: 'ask', target: 'rt_7', message: 'hi', timeout_ms: 1e21 }))
      .toThrow('property "timeout_ms" must be an integer')
    // 2^58+1 fits int64, so both routers reject it on the shared maximum bound.
    expect(() => assertMessageInput({ action: 'ask', target: 'rt_7', message: 'hi', timeout_ms: 288230376151711745 }))
      .toThrow('property "timeout_ms" must be at most 600000')
  })

  it('rejects route shapes the normaliser cannot reproduce faithfully', () => {
    expect(() => normalizeRouteRules([
      { type: 'enabledWhen', field: 'target', when: { op: 'mystery' } },
    ])).toThrow('unsupported expression op "mystery"')
    expect(() => normalizeRouteRules([
      { type: 'enabledWhen', field: 'target', when: { op: 'not', exprs: [{ op: 'cond', condition: 'a' }, { op: 'cond', condition: 'b' }] } },
    ])).toThrow('a not rule must carry exactly one expression')
    expect(() => normalizeRouteRules([
      { type: 'anyOf', rules: [{ type: 'check', field: 'action' }] },
    ])).toThrow('only the top-level action enum check rule can be omitted')
  })

  it('reads the peers action union and name limits from the published schema', () => {
    const variants = (peersInputSchema as { oneOf: Array<{ properties: Record<string, unknown> }> }).oneOf
    const schemaActions = variants.map(variant => (variant.properties.action as { const: string }).const).sort()
    expect(schemaActions).toEqual(['last', 'list', 'rename'])
    expect(() => assertPeersInput({})).not.toThrow()
    expect(() => assertPeersInput({ action: 'rename', name: 'x'.repeat(65) })).toThrow('must be at most 64 characters')
    expect(() => assertPeersInput({ action: 'rename', name: 'child\u200ba' })).toThrow('not allowed')
    expect(() => assertPeersInput({ action: 'last' })).toThrow('missing required property "target"')
    expect(() => assertPeersInput({ action: 5 })).toThrow('not a known action (list, rename, last)')
  })
})

describe('outcome text totality', () => {
  const contract = outcomeSchema as unknown as {
    outcomes: Array<{ outcome: string; render: Record<string, string> }>
    matrix: Record<string, string[]>
  }

  it('declares every vocabulary outcome from the contract', () => {
    expect(new Set(MESSAGE_OUTCOMES)).toEqual(new Set(contract.outcomes.map(row => row.outcome)))
  })

  it('has a render template for every matrix row', () => {
    for (const [action, outcomes] of Object.entries(contract.matrix)) {
      for (const outcome of outcomes) {
        expect(
          contract.outcomes.some(row => row.outcome === outcome && row.render[action] !== undefined),
          `${action}:${outcome} has no render template`,
        ).toBe(true)
      }
    }
  })

  it('renders every failure action the contract declares', () => {
    for (const action of ['tell', 'ask', 'reply', 'withdraw']) {
      expect(renderOutcome(action, 'not-sent', { message_id: '"m"', target: '"t"' })).not.toBe('')
      expect(renderOutcome(action, 'auth-rejected', { message_id: '"m"', target: '"t"' })).not.toBe('')
    }
  })
})

describe('last text', () => {
  const facts = { run_id: '"rt_7"', label: '"mesh-a"', stop_reason: 'end_turn' }

  it('renders the four peers last templates', () => {
    expect(renderLast('header', facts)).toBe(
      '**Last message from "mesh-a"** (run "rt_7", stop reason: end_turn)',
    )
    expect(renderLast('empty', facts)).toBe('run "rt_7" has no completed turn yet')
    expect(renderLast('no_text', facts)).toBe('run "rt_7" finished (stop reason: end_turn) with no final text')
    expect(renderLast('truncated_suffix', facts)).toBe(' (truncated)')
  })

  it('maps pi stop reasons onto the canonical words', () => {
    expect(normalizeTurnStopReason('stop')).toBe('end_turn')
    expect(normalizeTurnStopReason('end_of_turn')).toBe('end_turn')
    expect(normalizeTurnStopReason('length')).toBe('max_tokens')
    expect(normalizeTurnStopReason('aborted')).toBe('cancelled')
    expect(normalizeTurnStopReason('error')).toBe('error')
  })

  it('quotes the ids and sanitizes the stop reason like the Go renderer', () => {
    expect(quote('a\u200bb\u202ec\u0085d"e\\f\ng')).toBe(
      '"a\\u200bb\\u202ec\\u0085d\\"e\\\\f\\ng"',
    )
  })
})

describe('router invariants', () => {
  it('restores the ask claim unless the reply was delivered', async () => {
    for (const [outcome, restored] of [
      ['replied', false],
      ['not-sent', true],
      ['possibly-pending', true],
      ['edge-absent', true],
    ] as const) {
      const claimed = new Map<string, string>([['av1.reply.1', 'run-asker']])
      const provider: MessageProvider = {
        caller: () => ({ kind: 'broker', runId: 'run-caller' }),
        surface: () => 'pi-child',
        role: () => 'agent',
        resolve: async () => ({ runId: 'rt_7', match: 'unique' }),
        state: async () => ({ live: false, resumable: false, status: 'done' }),
        tell: async () => ({ outcome: 'sent' }),
        ask: async () => ({ outcome: 'sent' }),
        reply: async () => ({ outcome }),
        withdraw: async () => ({ outcome: 'withdrawn' }),
        resume: async () => ({ outcome: 'resumed' }),
        rename: async () => ({ outcome: 'renamed' }),
        last: async () => ({ outcome: 'sent' }),
        peers: async () => ({ peers: [] }),
        claimAsk: id => {
          const asker = claimed.get(id)
          claimed.delete(id)
          return asker
        },
        restoreAsk: (id, asker) => { claimed.set(id, asker) },
      }
      await executeMessage(provider, { action: 'reply', reply_to: 'av1.reply.1', message: 'OK' })
      expect(claimed.has('av1.reply.1'), outcome).toBe(restored)
    }
  })

  it('denies a resume before reading run state', async () => {
    let stateReads = 0
    const provider: MessageProvider = {
      caller: () => ({ kind: 'broker', runId: 'run-caller' }),
      surface: () => 'channeltools',
      role: () => 'agent',
      resolve: async () => ({ runId: 'rt_9', label: 'mesh-b', match: 'unique' }),
      state: async () => {
        stateReads++
        throw new Error('state must not be read')
      },
      tell: async () => ({ outcome: 'sent' }),
      ask: async () => ({ outcome: 'sent' }),
      reply: async () => ({ outcome: 'sent' }),
      withdraw: async () => ({ outcome: 'withdrawn' }),
      resume: async () => ({ outcome: 'resumed' }),
      rename: async () => ({ outcome: 'renamed' }),
      last: async () => ({ outcome: 'sent' }),
      peers: async () => ({ peers: [] }),
    }
    await expect(executeMessage(provider, { action: 'resume', target: 'rt_9', message: 'again' }))
      .rejects.toThrow('a broker-authenticated run may resume only a run it spawned')
    expect(stateReads).toBe(0)
  })

  it('leaves an in-vocabulary unlisted outcome unrendered instead of rewriting it', async () => {
    const provider = fakeProvider({
      name: 'tell returns lost',
      input: { action: 'tell', target: 'mesh-a', message: 'hi' },
      conditions: { callerKind: 'broker', surface: 'channeltools', targetLive: true, targetMatch: 'unique' },
      provider: { resolved: { run_id: 'rt_7', label: 'mesh-a' }, outcome: 'lost' },
      expect: { outcome: 'lost', message: '' },
    })
    const result = await executeMessage(provider, { action: 'tell', target: 'mesh-a', message: 'hi' })
    expect(result.outcome).toBe('lost')
    expect(result.text).toBe('')
  })

  it('refuses a resume whose registration fails and skips onResumed', async () => {
    let resumedHook = 0
    const provider: MessageProvider = {
      caller: () => ({ kind: 'owner', runId: 'supervisor' }),
      surface: () => 'pi-host',
      role: () => 'host',
      resolve: async () => ({ runId: 'rt_9', label: 'mesh-b', match: 'unique' }),
      state: async () => ({ live: false, resumable: true, status: 'done' }),
      tell: async () => ({ outcome: 'sent' }),
      ask: async () => ({ outcome: 'sent' }),
      reply: async () => ({ outcome: 'sent' }),
      withdraw: async () => ({ outcome: 'withdrawn' }),
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-77',
        runtimeId: 'rt_77',
        label: 'mesh-b-2',
        sentinelFile: '/runs/run-77/sentinel.done',
        onEvent: '/runs/run-77/events.log',
      }),
      rename: async () => ({ outcome: 'renamed' }),
      last: async () => ({ outcome: 'sent' }),
      peers: async () => ({ peers: [] }),
      registerResumed: () => {
        throw new Error('label "mesh-b-2" already exists')
      },
      onResumed: () => {
        resumedHook++
      },
    }
    await expect(executeMessage(provider, { action: 'resume', target: 'mesh-b', message: 'again', label: 'mesh-b-2' }))
      .rejects.toThrow(/resume succeeded but registration failed for run_id "run-77" runtime_id "rt_77"/)
    expect(resumedHook).toBe(0)
  })
})

/**
 * The shared expected objects in schemas/message_route.result.json are the
 * cross-surface fixture: the two MCP hosts and the OpenCode plugin assert their
 * tool results against the same file, so no surface can drift in casing or
 * nesting.
 */
describe('the shared surface result objects', () => {
  const surfaceFixture = resultFixture as unknown as {
    message: Record<string, Record<string, unknown>>
    peers: Record<string, Record<string, unknown>>
  }

  function surfaceProvider(overrides: Partial<MessageProvider> = {}): MessageProvider {
    return {
      caller: () => ({ kind: 'owner', runId: 'supervisor' }),
      surface: () => 'mcp-ts',
      role: () => 'host',
      resolve: async (ref: string) => ({ runId: ref, label: ref, match: 'unique' }),
      state: async () => ({ live: true, resumable: true, status: 'running' }),
      tell: async () => ({ outcome: 'sent', messageId: 'm-1' }),
      ask: async () => ({ outcome: 'sent' }),
      reply: async () => ({ outcome: 'replied' }),
      withdraw: async () => ({ outcome: 'withdrawn' }),
      resume: async () => ({
        outcome: 'resumed',
        runId: 'run-2',
        label: 'again',
        sentinelFile: '/runs/run-2/sentinel.done',
        onEvent: '/runs/run-2/events.log',
      }),
      rename: async () => ({ outcome: 'renamed', runId: 'rt_2', label: 'mesh-renamed', previousLabel: 'mesh-a' }),
      last: async () => ({ outcome: 'sent', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' }),
      peers: async () => ({
        peers: [
          { run_id: 'rt_2', label: 'mesh-a', kind: 'agent', status: 'running' },
          { run_id: 'rt_3' },
        ],
      }),
      ...overrides,
    }
  }

  it('serialises message results to the shared snake_case object', async () => {
    expect(resultOutput(await executeMessage(
      surfaceProvider({ state: async () => ({ live: true, resumable: false, status: 'running' }) }),
      { action: 'tell', target: 'mesh-a', message: 'hi' },
    ))).toEqual(surfaceFixture.message.tell)
    expect(resultOutput(await executeMessage(
      surfaceProvider({ state: async () => ({ live: false, resumable: true, status: 'done' }) }),
      { action: 'resume', target: 'mesh-a', message: 'again' },
    ))).toEqual(surfaceFixture.message.resume)
    expect(resultOutput(await executeMessage(surfaceProvider(), { action: 'reply', reply_to: 'a-1', message: 'pong' })))
      .toEqual(surfaceFixture.message.reply_edge_absent)
  })

  it('serialises peers results to the shared detail object', async () => {
    expect(peersDetails(await executePeers(surfaceProvider(), {}))).toEqual(surfaceFixture.peers.list)
    expect(peersDetails(await executePeers(surfaceProvider(), { action: 'rename', name: 'mesh-renamed' })))
      .toEqual(surfaceFixture.peers.rename)
    expect(peersDetails(await executePeers(
      surfaceProvider({ resolve: async () => ({ runId: 'rt_7', label: 'mesh-a', match: 'unique' }) }),
      { action: 'last', target: 'mesh-a' },
    ))).toEqual(surfaceFixture.peers.last)
  })
})

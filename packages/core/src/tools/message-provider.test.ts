import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import {
  createChildMessageProvider,
  createHostMessageProvider,
  installAskClaims,
  isGenerationMismatchError,
  type MessageClient,
} from './message-provider.js'
import { RpcError } from '../client.js'
import { forgetExternalRuns, findExternalRunById, registerExternalRun } from './run-registry.js'
import { executeMessage, executePeers, type ProviderResult } from '../message.js'

function fakeClient(overrides: Partial<MessageClient> = {}): MessageClient & {
  send: ReturnType<typeof mock>
  wait: ReturnType<typeof mock>
  reply: ReturnType<typeof mock>
  withdraw: ReturnType<typeof mock>
  resume: ReturnType<typeof mock>
  lastSet: ReturnType<typeof mock>
} {
  const send = mock(async (params: any) => ({ outcome: 'sent', message_id: params.message_id }))
  const wait = mock(async () => ({ outcome: 'replied', reply: { message_id: 'm', from_run_id: 'rt_7', payload: { message: 'OK' } } }))
  const reply = mock(async () => ({ outcome: 'replied' }))
  const withdraw = mock(async () => ({ outcome: 'withdrawn' }))
  const resume = mock(async (params: any) => ({
    run_id: params.run_id,
    label: params.label ?? 'resumed',
    runtime_id: 'rt_77',
    session_id: 'ses_7',
    sentinel_file: params.sentinel_file,
    on_event: params.on_event,
  }))
  const lastSet = mock(async () => {})
  const merged = {
    messageSend: send,
    messageWait: wait,
    messageReply: reply,
    messageWithdraw: withdraw,
    messagePeers: mock(async () => ({
      peers: [
        { run_id: 'supervisor', session: { run_id: 'supervisor', status: 'running' } },
        { run_id: 'rt_7', session: { run_id: 'rt_7', label: 'mesh-a', status: 'done' } },
      ],
    })),
    resume,
    sessionRename: mock(async () => ({ run_id: 'rt_1', label: 'new', previous_label: 'old' })),
    sessionLast: mock(async () => ({ run_id: 'rt_1', label: 'peer', has_record: false, final_output: '' })),
    sessionLastSet: lastSet,
    status: mock(async () => ({ status: 'done', session_id: 'ses_1' })),
    ...overrides,
  }
  return {
    ...merged,
    send: merged.messageSend,
    wait: merged.messageWait,
    reply: merged.messageReply,
    withdraw: merged.messageWithdraw,
    resume: merged.resume,
    lastSet: merged.sessionLastSet,
  } as any
}

function directoryEntries(dir: string): string[] {
  try {
    return fs.readdirSync(dir)
  } catch {
    return []
  }
}

describe('host message provider', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-provider-'))
  const previousHome = process.env.AVENOR_HOME
  beforeEach(() => {
    process.env.AVENOR_HOME = home
  })
  afterEach(() => {
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
  })

  function provider(client: MessageClient) {
    return createHostMessageProvider({
      surface: 'pi-host',
      supervisorId: '/tmp/avenor-provider.sock',
      generation: 'gen-1',
      client,
    })
  }

  it('sends a tell as the supervisor with no reply expectation', async () => {
    const client = fakeClient()
    const result = await executeMessage(provider(client), { action: 'tell', target: 'supervisor', message: 'hi' })
    expect(result.outcome).toBe('sent')
    const params = client.send.mock.calls[0][0] as any
    expect(params).toMatchObject({ to_run_id: 'supervisor', generation: 'gen-1', no_reply: true })
    expect(JSON.parse(params.body)).toMatchObject({ from: 'supervisor', from_run_id: 'supervisor', message: 'hi' })
  })

  it('asks with the reply expectation and renders the reply body', async () => {
    const client = fakeClient()
    const result = await executeMessage(provider(client), {
      action: 'ask',
      target: 'supervisor',
      message: 'question',
      timeout_ms: 1000,
    })
    expect(result.outcome).toBe('replied')
    expect(result.reply).toBe('OK')
    const send = client.send.mock.calls[0][0] as any
    expect(JSON.parse(send.body)).toMatchObject({ expects_reply: true, message: 'question' })
    expect(client.wait.mock.calls[0][0]).toMatchObject({ timeout_ms: 1000, generation: 'gen-1' })
  })

  it('mints the resume id and paths and forwards the idempotency key verbatim', async () => {
    const client = fakeClient()
    const result = await executeMessage(provider(client), {
      action: 'resume',
      target: 'rt_7',
      message: 'again',
      idempotency_key: 'k1',
    })
    expect(result.outcome).toBe('resumed')
    const params = client.resume.mock.calls[0][0] as any
    expect(params.idempotency_key).toBe('k1')
    expect(params.sentinel_file).toContain('sentinel.done')
    expect(params.on_event).toContain('events.log')
    expect(result.sentinelFile).toBe(params.sentinel_file)
    expect(result.onEvent).toBe(params.on_event)
  })

  it('publishes the host turn to the supervisor session', async () => {
    const client = fakeClient()
    const host = provider(client)
    await host.recordLastTurn?.('the answer', 'end_turn', false)
    expect(client.lastSet).toHaveBeenCalledWith('supervisor', 'the answer', 'end_turn', false)
  })

  it('resolves the new label after a rename, and an old-label-shaped public id by ID only', async () => {
    const supervisorId = '/tmp/avenor-provider.sock'
    const client = fakeClient({
      messagePeers: mock(async () => ({
        peers: [{ run_id: 'rt_1', session: { run_id: 'rt_1', label: 'new-name', status: 'done' } }],
      })),
    })
    process.env.AVENOR_HOME = home
    fs.mkdirSync(path.join(home, 'runs', 'old-name'), { recursive: true, mode: 0o700 })
    const host = provider(client)
    expect(await host.resolve('new-name')).toMatchObject({ runId: 'rt_1', label: 'new-name' })
    expect((await host.resolve('old-name')).match).toBe('none')

    registerExternalRun({
      runId: 'old-name',
      label: 'another-run',
      supervisorId,
      sentinelPath: path.join(home, 'runs', 'old-name', 'sentinel.done'),
      eventLogPath: path.join(home, 'runs', 'old-name', 'events.log'),
      runtimeId: 'rt_9',
    })
    try {
      expect(await host.resolve('old-name')).toMatchObject({ runId: 'rt_9', publicRunId: 'old-name' })
    } finally {
      forgetExternalRuns(supervisorId)
    }
  })

  it('lists the peers the broker reports', async () => {
    const client = fakeClient()
    const result = await executePeers(provider(client), { action: 'list' })
    expect(result.text).toBe('')
    expect(result.peers.map(peer => peer.run_id)).toEqual(['supervisor', 'rt_7'])
  })

  it('keeps the peers a partial listing read and refuses the rest', async () => {
    const client = fakeClient({
      messagePeers: mock(async () => ({
        peers: [{ run_id: 'rt_7', session: { run_id: 'rt_7', label: 'mesh-a', status: 'done' } }],
        peers_error: 'the broker could not read every peer',
      })),
    })
    const host = provider(client)
    expect(await host.resolve('rt_7')).toMatchObject({ runId: 'rt_7', match: 'unique' })
    await expect(host.resolve('rt_missing')).rejects.toThrow('the peer listing is incomplete')
    const listed = await host.peers()
    expect(listed.peers.map(peer => peer.run_id)).toEqual(['rt_7'])
    expect(listed.detail).toBe('the broker could not read every peer')
  })

  it('returns a rejected host send as the transport outcome, not a refusal', async () => {
    const client = fakeClient({
      messageSend: mock(async () => {
        throw new RpcError(-32000, 'messaging: generation mismatch', {
          code: 'generation-mismatch',
          outcome: 'not-sent',
          wire_code: 'generation-mismatch',
        })
      }),
    })
    const result = await executeMessage(provider(client), { action: 'tell', target: 'supervisor', message: 'hi' })
    expect(result.outcome).toBe('not-sent')
    expect(result.text).toBe('message "av1.test.1" not sent to "supervisor"; nothing is pending'.replace('av1.test.1', result.messageId ?? ''))
  })

  it('reports a reply transport failure as not-sent and leaves the ask claimable', async () => {
    const pending = new Map([['ask-1', { from_run_id: 'rt_7', message_id: 'ask-1' }]])
    const client = fakeClient({
      messageReply: mock(async () => {
        throw new RpcError(-32000, 'messaging: target unavailable', {
          code: 'target-unavailable',
          outcome: 'not-sent',
          wire_code: 'target-unavailable',
        })
      }),
    })
    const host = createHostMessageProvider({
      surface: 'pi-host',
      supervisorId: '/tmp/avenor-provider.sock',
      generation: 'gen-1',
      client,
      pendingAsks: pending,
      pollGeneration: () => 1,
    })
    const result = await executeMessage(host, { action: 'reply', reply_to: 'ask-1', message: 'OK' })
    expect(result.outcome).toBe('not-sent')
    expect(result.text).toBe('reply to "ask-1" was not sent; nothing is pending')
    expect(pending.has('ask-1')).toBe(true)
  })

  it('reports a fence-capacity withdrawal as possibly-pending', async () => {
    const client = fakeClient({
      messageWithdraw: mock(async () => {
        throw new RpcError(-32000, 'messaging: fence capacity', {
          code: 'edge-absent',
          outcome: 'edge-absent',
          wire_code: 'fence-capacity',
        })
      }),
    })
    const result = await executeMessage(provider(client), { action: 'withdraw', message_id: 'av1.w.1' })
    expect(result.outcome).toBe('possibly-pending')
    expect(result.text).toBe('ask "av1.w.1" may still be pending; retry avenor_message action="withdraw" message_id="av1.w.1"')
  })

  it('withdraws a wait that ended without a reply', async () => {
    const client = fakeClient({
      messageWait: mock(async () => ({ outcome: 'timed-out', message_id: 'm' })),
    })
    const result = await executeMessage(provider(client), {
      action: 'ask',
      target: 'supervisor',
      message: 'question',
      timeout_ms: 50,
    })
    expect(result.outcome).toBe('timed-out')
    expect(client.withdraw).toHaveBeenCalled()
  })

  it('withdraws an ask when the tool call aborts', async () => {
    const client = fakeClient({
      messageWait: mock(() => new Promise(() => {})),
    })
    const controller = new AbortController()
    const pending = executeMessage(provider(client), {
      action: 'ask',
      target: 'supervisor',
      message: 'question',
      timeout_ms: 10_000,
    }, { signal: controller.signal })
    controller.abort(new Error('cancelled'))
    const result = await pending
    expect(result.outcome).toBe('possibly-pending')
    expect(client.withdraw).toHaveBeenCalledWith({ message_id: result.messageId, generation: 'gen-1' })
  })

  it('renders a reply payload without message as the full envelope', async () => {
    const client = fakeClient({
      messageWait: mock(async () => ({
        outcome: 'replied',
        message_id: 'm',
        reply: { message_id: 'm', from_run_id: 'rt_7', payload: { result: 'opaque' } },
      })),
    })
    const result = await executeMessage(provider(client), { action: 'ask', target: 'supervisor', message: 'question' })
    expect(result.reply).toBe('{"from_run_id":"rt_7","message_id":"m","payload":{"result":"opaque"}}')
  })

  it('renders a null or empty payload as the full envelope, not as an empty reply', async () => {
    for (const [payload, rendered] of [
      [null, '{"from_run_id":"rt_7","message_id":"m","payload":null}'],
      ['', '{"from_run_id":"rt_7","message_id":"m","payload":""}'],
    ] as const) {
      const client = fakeClient({
        messageWait: mock(async () => ({
          outcome: 'replied',
          message_id: 'm',
          reply: { message_id: 'm', from_run_id: 'rt_7', payload },
        })),
      })
      const result = await executeMessage(provider(client), { action: 'ask', target: 'supervisor', message: 'question' })
      expect(result.reply).toBe(rendered)
    }
  })

  it('fills the public run id when a label resolves by runtime id', async () => {
    const supervisorId = '/tmp/avenor-provider.sock'
    fs.mkdirSync(path.join(home, 'runs', 'run-42'), { recursive: true, mode: 0o700 })
    registerExternalRun({
      runId: 'run-42',
      label: 'mesh-a',
      supervisorId,
      sentinelPath: path.join(home, 'runs', 'run-42', 'sentinel.done'),
      eventLogPath: path.join(home, 'runs', 'run-42', 'events.log'),
      runtimeId: 'rt_7',
    })
    try {
      const host = provider(fakeClient())
      expect(await host.resolve('mesh-a')).toMatchObject({ runId: 'rt_7', publicRunId: 'run-42', label: 'mesh-a' })
      const last = await executePeers(host, { action: 'last', target: 'mesh-a' })
      expect(last.last?.run_id).toBe('run-42')
      expect(last.last?.runtime_id).toBe('rt_7')
    } finally {
      forgetExternalRuns(supervisorId)
    }
  })

  it('reports a finished sentinel as resumable when there is no session', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-sentinel-'))
    try {
      const sentinelFile = path.join(dir, 'sentinel.done')
      fs.writeFileSync(sentinelFile, 'DONE\nSESSION_ID=ses_1\n')
      const client = fakeClient({
        status: mock(async () => ({ status: 'ended', sentinel_file: sentinelFile })),
      })
      expect(await provider(client).state('run-1')).toMatchObject({ live: false, resumable: true, status: 'done' })
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('refuses a finished sentinel whose status is not resumable', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-sentinel-'))
    try {
      const sentinelFile = path.join(dir, 'sentinel.done')
      fs.writeFileSync(sentinelFile, 'TIMEOUT\nSESSION_ID=ses_1\n')
      const client = fakeClient({
        status: mock(async () => ({ status: 'ended', sentinel_file: sentinelFile })),
      })
      expect(await provider(client).state('run-1')).toMatchObject({ live: false, resumable: false, status: 'done' })
    } finally {
      fs.rmSync(dir, { recursive: true, force: true })
    }
  })

  it('keeps a resumed registration when the status read fails', async () => {
    const supervisorId = '/tmp/avenor-provider.sock'
    const runDir = path.join(home, 'runs', 'resumed-1')
    fs.mkdirSync(runDir, { recursive: true, mode: 0o700 })
    const client = fakeClient({
      status: mock(async () => {
        throw new Error('status unavailable')
      }),
    })
    const host = provider(client)
    try {
      await host.registerResumed?.({
        outcome: 'resumed',
        runId: 'resumed-1',
        label: 'again',
        runtimeId: 'rt_9',
        sentinelFile: path.join(runDir, 'sentinel.done'),
        onEvent: path.join(runDir, 'events.log'),
      })
      expect(findExternalRunById(supervisorId, 'resumed-1')).toBeDefined()
    } finally {
      forgetExternalRuns(supervisorId)
      fs.rmSync(runDir, { recursive: true, force: true })
    }
  })

  it('restores an ask only under the poll generation that claimed it', async () => {
    let generation = 1
    const pending = new Map([['ask-1', { from_run_id: 'rt_7', message_id: 'ask-1' }]])
    const host = createHostMessageProvider({
      surface: 'pi-host',
      supervisorId: '/tmp/avenor-provider.sock',
      generation: 'gen-1',
      client: fakeClient(),
      pendingAsks: pending,
      pollGeneration: () => generation,
    })
    expect(host.claimAsk?.('ask-1')).toBe('rt_7')
    host.restoreAsk?.('ask-1', 'rt_7')
    expect(pending.has('ask-1')).toBe(true)
    expect(host.claimAsk?.('ask-1')).toBe('rt_7')
    generation = 2
    host.restoreAsk?.('ask-1', 'rt_7')
    expect(pending.has('ask-1')).toBe(false)
  })

  it('releases a claim the surface owns once the reply is accepted', async () => {
    const pending = new Map([['ask-1', { from_run_id: 'rt_7', message_id: 'ask-1' }]])
    // A surface that keeps claims across provider rebuilds installs its own map.
    const surfaceClaims: Map<string, { asker: string, generation: number }> = new Map()
    let claimedWhileSending = -1
    const client = fakeClient({
      messageReply: mock(async () => {
        claimedWhileSending = surfaceClaims.size
        return { outcome: 'replied' }
      }),
    })
    const host = createHostMessageProvider({
      surface: 'pi-host',
      supervisorId: '/tmp/avenor-provider.sock',
      generation: 'gen-1',
      client,
      pendingAsks: pending,
      pollGeneration: () => 0,
    })
    installAskClaims(host, surfaceClaims, pending, () => 0)

    const result = await executeMessage(host, { action: 'reply', reply_to: 'ask-1', message: 'answer' })

    expect(result.outcome).toBe('replied')
    expect(claimedWhileSending).toBe(1)
    expect(surfaceClaims.size).toBe(0)
  })

  it('reads a plain error naming a generation mismatch as stale', () => {
    expect(isGenerationMismatchError(new Error('messaging: generation mismatch'))).toBe(true)
    expect(isGenerationMismatchError(new Error('some other error'))).toBe(false)
  })

  it('removes the run directory a failed resume created', async () => {
    const freshHome = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-resume-fail-'))
    process.env.AVENOR_HOME = freshHome
    try {
      const client = fakeClient({
        resume: mock(async () => {
          throw new Error('resume refused')
        }),
      })
      await expect(
        executeMessage(provider(client), { action: 'resume', target: 'rt_7', message: 'again' }),
      ).rejects.toThrow()
      expect(directoryEntries(path.join(freshHome, 'runs'))).toEqual([])
    } finally {
      process.env.AVENOR_HOME = home
      fs.rmSync(freshHome, { recursive: true, force: true })
    }
  })

  it('registers a successful resume and runs the surface hook', async () => {
    const supervisorId = '/tmp/avenor-provider.sock'
    const onResumed = mock((_result: ProviderResult) => {})
    const host = createHostMessageProvider({
      surface: 'pi-host',
      supervisorId,
      generation: 'gen-1',
      client: fakeClient(),
      onResumed,
    })
    let runId: string | undefined
    try {
      const result = await executeMessage(host, { action: 'resume', target: 'rt_7', message: 'again' })
      runId = result.runId
      expect(onResumed).toHaveBeenCalledTimes(1)
      const resumed = onResumed.mock.calls[0][0]
      expect(resumed).toMatchObject({ outcome: 'resumed', runId: result.runId })
      expect(findExternalRunById(supervisorId, result.runId ?? '')).toBeDefined()
    } finally {
      forgetExternalRuns(supervisorId)
      if (runId) fs.rmSync(path.join(home, 'runs', runId), { recursive: true, force: true })
    }
  })
})

describe('child message provider', () => {
  function childBroker(overrides: Record<string, unknown> = {}) {
    return {
      tell: mock(async () => ({ outcome: 'sent', messageId: 'm' })),
      ask: mock(async () => ({ outcome: 'replied', messageId: 'm', reply: 'OK' })),
      reply: mock(async () => ({ outcome: 'replied', messageId: 'm' })),
      withdraw: mock(async () => ({ outcome: 'withdrawn' })),
      peers: mock(async () => ({
        peers: [
          { run_id: 'supervisor', label: 'main', status: 'running' },
          { run_id: 'rt_2', label: 'mesh-a', status: 'done' },
        ],
      })),
      rename: mock(async () => ({ outcome: 'renamed', runId: 'rt_2', label: 'new', previousLabel: 'mesh-a' })),
      last: mock(async () => ({ outcome: 'sent', label: 'mesh-a', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' })),
      ...overrides,
    } as any
  }

  it('resolves a peer by runtime id and by label', async () => {
    const child = createChildMessageProvider({ surface: 'pi-child', runId: 'rt_child', broker: childBroker() })
    expect((await child.resolve('rt_2'))).toMatchObject({ runId: 'rt_2', label: 'mesh-a', match: 'unique' })
    expect((await child.resolve('mesh-a'))).toMatchObject({ runId: 'rt_2', match: 'unique' })
    expect((await child.resolve('ghost')).match).toBe('none')
  })

  it('is never resumable and denies a resume defensively', async () => {
    const child = createChildMessageProvider({ surface: 'pi-child', runId: 'rt_child', broker: childBroker() })
    expect((await child.state('rt_2')).resumable).toBe(false)
    await expect(child.resume({ target: 'rt_2', message: 'x' })).rejects.toMatchObject({ code: 'not_permitted' })
  })

  it('claims a pending ask once and passes the asker to the broker', async () => {
    const pending = new Map([['ask-1', { from_run_id: 'supervisor', message_id: 'ask-1' }]])
    const broker = childBroker()
    const child = createChildMessageProvider({
      surface: 'pi-child',
      runId: 'rt_child',
      broker,
      pendingAsks: pending,
    })
    const result = await executeMessage(child, { action: 'reply', reply_to: 'ask-1', message: 'answer' })
    expect(result.outcome).toBe('replied')
    expect(broker.reply).toHaveBeenCalledWith('supervisor', 'ask-1', 'answer')
    expect(pending.has('ask-1')).toBe(false)
  })
})

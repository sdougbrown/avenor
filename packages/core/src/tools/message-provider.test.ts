import { afterEach, describe, expect, it, mock } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { createChildMessageProvider, createHostMessageProvider, type MessageClient } from './message-provider.js'
import { forgetExternalRuns, registerExternalRun } from './run-registry.js'
import { executeMessage, executePeers } from '../message.js'

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
  return {
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
    send,
    wait,
    reply,
    withdraw,
    resume,
    lastSet,
  } as any
}

describe('host message provider', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-provider-'))
  const previousHome = process.env.AVENOR_HOME
  process.env.AVENOR_HOME = home
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

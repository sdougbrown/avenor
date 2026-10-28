import { expect, test } from 'bun:test'
import { messageTool } from '@dougbots/avenor-core'
import { makeMessageProvider, startExtensionHarness, waitFor } from './test-fixtures.js'

function broker() {
  const previous = globalThis.fetch
  const calls: Array<{ path: string; body: Record<string, unknown> }> = []
  const inbox: unknown[] = []
  let identity: Record<string, unknown> = {}
  let disconnected = false
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(url)).pathname
    const body = JSON.parse(String(init?.body ?? '{}'))
    calls.push({ path, body })
    if (disconnected) throw new Error('offline')
    if (path === '/register') { identity = body; return Response.json(body) }
    if (path === '/sessions') return Response.json({ generation: 'gen', sessions: [
      { run_id: identity.run_id, label: 'pi-host', status: 'running' },
      { run_id: 'claude-peer', label: 'claude', status: 'running' },
    ] })
    if (path === '/poll-control') return Response.json(body.run_id === identity.run_id ? inbox.splice(0) : [])
    if (path === '/send') return Response.json({ outcome: body.payload.reply_to ? 'replied' : 'sent' })
    return Response.json({})
  }, { preconnect() {} }) as typeof fetch
  return { calls, inbox, disconnect: () => { disconnected = true }, reconnect: () => { disconnected = false }, restore: () => { globalThis.fetch = previous } }
}

test('top-level peer receives and replies centrally while local child tools keep their provider', async () => {
  const b = broker()
  const sent: string[] = []
  const local = makeMessageProvider({
    peers: async () => ({ peers: [{ run_id: 'child', label: 'local-child', status: 'running' }] }),
    resolve: async ref => ref === 'child' ? { runId: ref, match: 'unique' } : { runId: '', match: 'none' },
    state: async () => ({ live: true, resumable: false, status: 'running' }),
    tell: async target => { sent.push(target); return { outcome: 'sent' } },
  })
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ sessionName: () => 'pi-host', deps: { messageTool, hostProviderFor: async () => local }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    const tools = h.registeredTools
    const peers = await tools.avenor_peers.execute('peers', {})
    expect(peers.details.peers.map((p: { label: string }) => p.label)).toEqual(['local-child', 'pi-host', 'claude'])
    b.inbox.push({ type: 'agent_message', payload: { id: 'inbound', from_run_id: 'claude-peer', message: 'ping', expects_reply: true } })
    await waitFor(() => h!.sendUserMessage.mock.calls.length > 0)
    expect((await tools.avenor_message.execute('reply', { action: 'reply', reply_to: 'inbound', message: 'pong' })).details.outcome).toBe('replied')
    const reply = b.calls.find(c => c.path === '/send')!.body
    expect(reply.from_run_id).toBe(b.calls.find(c => c.path === '/register')!.body.run_id)
    await tools.avenor_message.execute('tell', { action: 'tell', target: 'local-child', message: 'hello' })
    expect(sent).toEqual(['child'])
    expect(b.calls.find(c => c.path === '/session_name')?.body.name).toBe('pi-host')
  } finally {
    await h?.eventHandlers.session_shutdown({}, {})
    expect(b.calls.some(c => c.path === '/retire')).toBe(true)
    b.restore()
  }
})

test('central-only peer listing does not create a supervisor and tell does not invite replies', async () => {
  const b = broker()
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { messageTool, hostProviderFor: undefined, Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    const peers = await h.registeredTools.avenor_peers.execute('p', {})
    expect(peers.details.peers).toHaveLength(2)
    b.inbox.push({ type: 'agent_message', payload: { id: 'tell', from_run_id: 'claude-peer', message: 'hello' } })
    await waitFor(() => h!.sendUserMessage.mock.calls.length > 0)
    const frame = String(h.sendUserMessage.mock.calls[0][0])
    expect(frame).toContain('hello')
    expect(frame).not.toContain('reply_to')
    expect((await h.registeredTools.avenor_message.execute('r', { action: 'reply', reply_to: 'tell', message: 'bad' })).details.outcome).toBe('edge-absent')
    b.disconnect()
    const polls = b.calls.filter(c => c.path === '/poll-control').length
    await waitFor(() => b.calls.filter(c => c.path === '/poll-control').length > polls)
    b.reconnect()
    const result = await h.registeredTools.avenor_message.execute('tell', { action: 'tell', target: 'claude', message: 'back' })
    expect(result.details.outcome).toBe('sent')
    expect(b.calls.filter(c => c.path === '/register')).toHaveLength(1)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('registration failure never falls back to local messaging and retries the same identity', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const registrations: Array<Record<string, unknown>> = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/register') {
      registrations.push(JSON.parse(String(init?.body)))
      if (registrations.length === 1) throw new Error('response lost')
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    await expect(h.registeredTools.avenor_message.execute('t', { action: 'tell', target: 'local-child', message: 'unsafe' })).rejects.toThrow('registration has not succeeded')
    await waitFor(() => registrations.length === 2)
    expect(registrations[0]).toEqual(registrations[1])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('expired identity stops polling and refuses tools rather than re-registering', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let expired = false
  let refused = 0
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (expired && new URL(String(url)).pathname === '/poll-control' && JSON.parse(String(init?.body)).run_id === b.calls.find(c => c.path === '/register')?.body.run_id) {
      refused++
      return Response.json({ code: 'peer-expired' }, { status: 410 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    expired = true
    await waitFor(() => refused === 1)
    await expect(h.registeredTools.avenor_peers.execute('p', {})).rejects.toThrow('identity is unavailable')
    expect(b.calls.filter(c => c.path === '/register')).toHaveLength(1)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('configured central polling waits for an existing local supervisor before receiving local asks', async () => {
  const b = broker()
  let exists = false
  let receives = 0
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: {
        messageTool,
        Supervisor: class { static currentInstance() { return exists ? { supervisorId: 'local', getClient: () => ({}) } : undefined } } as never,
        brokerReceiveTool: async () => { receives++; return { asks: [] } },
      },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await waitFor(() => b.calls.filter(c => c.path === '/poll-control').length >= 3)
    expect(receives).toBe(0)
    exists = true
    await waitFor(() => receives > 0)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('retained drained batches retry failed injection in order and install reply claims once delivered', async () => {
  const b = broker()
  const delivered: string[] = []
  let attempts = 0
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { messageTool },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
      sendUserMessage: text => {
        attempts++
        if (attempts === 1) throw new Error('injection temporarily unavailable')
        delivered.push(text)
      },
    })
    for (const [id, message] of [['ask-first', 'first batch message'], ['ask-second', 'second batch message']]) {
      b.inbox.push({ type: 'agent_message', payload: { id, from_run_id: 'claude-peer', message, expects_reply: true } })
    }
    await waitFor(() => delivered.length === 2)
    expect(attempts).toBe(3)
    expect(delivered[0]).toContain('first batch message')
    expect(delivered[1]).toContain('second batch message')
    for (const id of ['ask-first', 'ask-second']) {
      const result = await h.registeredTools.avenor_message.execute(id, { action: 'reply', reply_to: id, message: 'answer ' + id })
      expect(result.details.outcome).toBe('replied')
    }
    expect(b.calls.filter(c => c.path === '/send').map(c => (c.body.payload as Record<string, unknown>).reply_to)).toEqual(['ask-first', 'ask-second'])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('completed top-level turns are readable through central self and preserve the local writer', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let record: Record<string, unknown> | null = null
  let exists = false
  const localRecords: unknown[][] = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/last_message') {
      if (init?.method === 'POST') { record = JSON.parse(String(init.body)); return Response.json({}) }
      return Response.json({ has_record: record !== null, final_output: record?.final_output ?? '', stop_reason: record?.stop_reason ?? '', final_output_truncated: record?.truncated ?? false })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: {
        messageTool,
        Supervisor: class { static currentInstance() { return exists ? { supervisorId: 'local', getClient: () => ({}) } : undefined } } as never,
        hostProviderFor: async () => makeMessageProvider({ recordLastTurn: async (...args) => { localRecords.push(args) } }),
      },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await h.eventHandlers.turn_end({ message: { role: 'assistant', content: 'central answer', stopReason: 'stop' } })
    expect(record).toMatchObject({ final_output: 'central answer', stop_reason: 'end_turn', truncated: false })
    const result = await h.registeredTools.avenor_peers.execute('last', { action: 'last', target: 'self' })
    expect(result.details.last.final_output).toBe('central answer')
    expect(localRecords).toHaveLength(0)
    exists = true
    await h.eventHandlers.turn_end({ message: { role: 'assistant', content: 'shared answer', stopReason: 'error' } })
    expect(record).toMatchObject({ final_output: 'shared answer', stop_reason: 'error' })
    expect(localRecords).toEqual([['shared answer', 'error', false]])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('queued central turn records keep order and cannot publish into a restarted identity', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const firstWrite = Promise.withResolvers<Response>()
  const records: Array<Record<string, unknown>> = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/last_message' && init?.method === 'POST') {
      records.push(JSON.parse(String(init.body)))
      return records.length === 1 ? firstWrite.promise : Response.json({})
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    const first = h.eventHandlers.turn_end({ message: { content: 'first', stopReason: 'stop' } })
    await waitFor(() => records.length === 1)
    const queued = h.eventHandlers.turn_end({ message: { content: 'stale second', stopReason: 'stop' } })
    await Promise.resolve()
    expect(records).toHaveLength(1)
    await h.eventHandlers.session_shutdown({}, {})
    await h.eventHandlers.session_start({}, h.ctx)
    await h.eventHandlers.turn_end({ message: { content: 'new identity', stopReason: 'stop' } })
    firstWrite.resolve(Response.json({}))
    await Promise.all([first, queued])
    expect(records.map(r => r.final_output)).toEqual(['first', 'new identity'])
    expect(records[0].run_id).not.toBe(records[1].run_id)
  } finally { firstWrite.resolve(Response.json({})); await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('latest completed turn survives initial registration failures and publishes after registration recovers', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let ready = false
  const records: string[] = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(url)).pathname
    if (path === '/register' && !ready) return Response.json({ error: 'unavailable' }, { status: 503 })
    if (path === '/last_message' && init?.method === 'POST') records.push(JSON.parse(String(init.body)).final_output)
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await h.eventHandlers.turn_end({ message: { content: 'old answer', stopReason: 'stop' } })
    await h.eventHandlers.turn_end({ message: { content: 'latest answer', stopReason: 'stop' } })
    ready = true
    await waitFor(() => records.length > 0)
    expect(records).toEqual(['latest answer'])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('failed last-turn publication retries latest content on polling recovery without another turn', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let ready = false
  const records: string[] = []
  let attempts = 0
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/last_message' && init?.method === 'POST') {
      attempts++
      if (!ready) throw new Error('connection reset')
      records.push(JSON.parse(String(init.body)).final_output)
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await h.eventHandlers.turn_end({ message: { content: 'old answer', stopReason: 'stop' } })
    await h.eventHandlers.turn_end({ message: { content: 'latest answer', stopReason: 'error' } })
    expect(attempts).toBeGreaterThanOrEqual(2)
    ready = true
    await waitFor(() => records.length > 0)
    expect(records).toEqual(['latest answer'])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('a pending turn from failed registration is cleared before a new session registers', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let ready = false
  const records: string[] = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(url)).pathname
    if (path === '/register' && !ready) return Response.json({}, { status: 503 })
    if (path === '/last_message' && init?.method === 'POST') records.push(JSON.parse(String(init.body)).final_output)
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await h.eventHandlers.turn_end({ message: { content: 'previous session', stopReason: 'stop' } })
    await h.eventHandlers.session_shutdown({}, {})
    ready = true
    await h.eventHandlers.session_start({}, h.ctx)
    await waitFor(() => b.calls.filter(c => c.path === '/poll-control').length >= 3)
    expect(records).toEqual([])
    await h.eventHandlers.turn_end({ message: { content: 'current session', stopReason: 'stop' } })
    expect(records).toEqual(['current session'])
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

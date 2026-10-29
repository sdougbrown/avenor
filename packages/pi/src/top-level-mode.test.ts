import { expect, mock, test } from 'bun:test'
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
    expect(frame).toContain('Message from peer')
    // The framing is what makes a peer's words read as a peer's, so it is pinned
    // here where the tell is actually delivered, not only on the formatter.
    expect(frame).toContain('another session')
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

test('an ask in flight is aborted by session shutdown instead of outliving its identity', async () => {
  const b = broker()
  const base = globalThis.fetch
  // Hold /wait_reply open until the session closes, recording whether the abort
  // signal its transport actually carried.
  const aborted = { value: false }
  const fetchWithPatientWait = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname !== '/wait_reply') return (base as typeof fetch)(url, init)
    return new Promise<Response>((_resolve, reject) => {
      const signal = init?.signal
      if (!signal) { reject(new Error('missing transport deadline')); return }
      const onAbort = () => { aborted.value = true; reject(signal.reason) }
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, { once: true })
    })
  }, { preconnect() {} }) as typeof fetch
  // Installed before the session starts: the central provider captures its
  // transport when it is built, so a later swap would not reach it.
  globalThis.fetch = fetchWithPatientWait
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { messageTool, hostProviderFor: undefined, Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    expect((await h.registeredTools.avenor_peers.execute('p', {})).details.peers).toHaveLength(2)
    const pending = h.registeredTools.avenor_message.execute('ask', { action: 'ask', target: 'claude', message: 'question', timeout_ms: 60000 })
    await waitFor(() => b.calls.some(c => c.path === '/send'))
    await h.eventHandlers.session_shutdown({}, {})
    const result = await pending.catch((error: unknown) => error)
    expect(aborted.value).toBe(true)
    // Shutdown withdraws the edge rather than leaving it to time out, so the ask
    // names the outcome the shutdown path produces, not merely "not replied".
    expect((result as { details?: { outcome?: string } }).details).toMatchObject({ action: 'ask', outcome: 'withdrawn' })
  } finally { globalThis.fetch = base; await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
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

test('a duplicate-poll refusal retries instead of ending the session', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let conflicts = 2
  let polls = 0
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/poll-control') {
      polls++
      if (conflicts-- > 0) return Response.json({ code: 'duplicate-consumer' }, { status: 409 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    // Two 409s arrive, then the polls continue: `duplicate-consumer` says another
    // poll was still in flight, which the next tick answers, not a dead identity.
    await waitFor(() => polls > 2)
    const peers = await h.registeredTools.avenor_peers.execute('p', {})
    expect(peers.details.peers.length).toBeGreaterThan(0)
    expect(b.calls.filter(c => c.path === '/register')).toHaveLength(1)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('a central failure reaches the session through notify, not raw stderr', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const originalError = console.error
  const errorLog = mock(() => {})
  console.error = errorLog as any
  let failing = false
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (failing && new URL(String(url)).pathname === '/poll-control') {
      return Response.json({ code: 'internal' }, { status: 500 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    failing = true
    await waitFor(() => h!.notify.mock.calls.length > 0)
    // One notice for the episode, on Pi's TUI-safe channel, and nothing raw.
    const notices = h.notify.mock.calls.map((call: unknown[]) => String(call[0]))
    expect(notices.filter(text => text.includes('central broker poll failed'))).toHaveLength(1)
    expect(errorLog.mock.calls.map((call: unknown[]) => String(call[0])).join('\n')).not.toContain('central broker poll failed')
    // The failure is on the bus too, so a session that drops the notice keeps a record.
    const peerErrors = h.emitted.filter(([channel]) => channel === 'avenor:peer:error')
    expect(peerErrors.length).toBeGreaterThan(0)
    expect(peerErrors[0][1]).toMatchObject({ operation: 'poll', terminal: false })
  } finally { console.error = originalError; await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

// Every status and code the poll path reads as terminal, in one table: a regression
// that drops one leaves the session polling a refused identity every two seconds.
for (const refusal of [
  { name: '401 unauthorized', status: 401, code: undefined },
  { name: '403 refused upstream', status: 403, code: undefined },
  { name: '410 peer-expired', status: 410, code: 'peer-expired' },
  { name: '409 peer-retired', status: 409, code: 'peer-retired' },
  { name: '409 registration-mismatch', status: 409, code: 'registration-mismatch' },
]) {
  test(`a ${refusal.name} refusal ends the session`, async () => {
    const b = broker()
    const delegate = globalThis.fetch
    let refused = 0
    globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
      if (new URL(String(url)).pathname === '/poll-control') {
        refused++
        return Response.json(refusal.code ? { code: refusal.code } : {}, { status: refusal.status })
      }
      return delegate(url, init)
    }, { preconnect() {} }) as typeof fetch
    let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
    try {
      h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
      await waitFor(() => refused === 1)
      await expect(h.registeredTools.avenor_peers.execute('p', {})).rejects.toThrow('identity is unavailable')
      // Terminal means one refusal, not a session that keeps asking.
      expect(refused).toBe(1)
      const peerErrors = h.emitted.filter(([channel]) => channel === 'avenor:peer:error')
      expect(peerErrors.at(-1)?.[1]).toMatchObject({ operation: 'poll', terminal: true })
    } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
  })
}

test('a codeless 409 keeps polling, because that is another poll in flight', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let polls = 0
  const ownRun = () => String(b.calls.find(c => c.path === '/register')?.body.run_id ?? '')
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? '{}'))
    if (new URL(String(url)).pathname === '/poll-control' && body.run_id === ownRun()) {
      polls++
      if (polls === 1) return Response.json({}, { status: 409 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test', pollIntervalMs: 20 } })
    await waitFor(() => polls > 2, 10000)
    const peers = await h.registeredTools.avenor_peers.execute('p', {})
    expect(peers.details.peers.length).toBeGreaterThan(0)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('a registration retried from the poll tick reports as a registration', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/register') {
      return Response.json({ code: 'internal' }, { status: 500 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { messageTool },
      extOptions: { topLevelBrokerUrl: 'https://broker.test', pollIntervalMs: 100 },
    })
    // Wait past the first report, which comes from session start, so the tick has
    // retried the registration at least twice and labelled it itself.
    await waitFor(() => h!.emitted.filter(([channel]) => channel === 'avenor:peer:error').length >= 3, 10000)
    // The same failure must not be filed under two operations depending on which
    // path hit it: `operation` is part of the payload companion extensions key on.
    const operations = h.emitted
      .filter(([channel]) => channel === 'avenor:peer:error')
      .map(([, payload]) => (payload as Record<string, unknown>).operation)
    expect(operations).toContain('register')
    expect(operations).not.toContain('poll')
    const notices = h.notify.mock.calls.map((call: unknown[]) => String(call[0]))
    expect(notices.some(text => text.includes('central peer registration failed'))).toBe(true)
    expect(notices.some(text => text.includes('central broker poll'))).toBe(false)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
}, 20000)

test('a refused session name is a notice on the peer channel, not raw stderr', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const errorLog = mock(() => {})
  const originalError = console.error
  console.error = errorLog as any
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/session_name') {
      return Response.json({ code: 'invalid-name' }, { status: 400 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    await waitFor(() => h!.notify.mock.calls.some(call => String(call[0]).includes('session_name_set refused')))
    expect(h.emitted.filter(([channel]) => channel === 'avenor:peer:error')
      .map(([, payload]) => payload as Record<string, unknown>)
      .some(payload => payload.operation === 'session-name' && payload.code === 'invalid-name')).toBe(true)
    expect(errorLog.mock.calls.map((call: unknown[]) => String(call[0])).join('\n')).not.toContain('session_name_set')
  } finally { console.error = originalError; await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

test('a failed turn publication notifies once per episode and re-arms after a success', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let failing = false
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (failing && new URL(String(url)).pathname === '/last_message' && init?.method === 'POST') {
      return Response.json({ code: 'internal' }, { status: 500 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  const publishNotices = () => h!.notify.mock.calls
    .map((call: unknown[]) => String(call[0]))
    .filter(text => text.includes('central last-turn publication failed'))
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    failing = true
    await h.eventHandlers.turn_end({ message: { content: 'first', stopReason: 'stop' } })
    await waitFor(() => publishNotices().length === 1)
    // A second failed turn inside the same episode stays quiet on the UI, but the
    // bus keeps the record.
    await h.eventHandlers.turn_end({ message: { content: 'second', stopReason: 'stop' } })
    await waitFor(() => h!.emitted.filter(([channel]) => channel === 'avenor:peer:error').length >= 2)
    expect(publishNotices()).toHaveLength(1)
    // A successful publish ends the episode.
    failing = false
    await h.eventHandlers.turn_end({ message: { content: 'third', stopReason: 'stop' } })
    await waitFor(() => b.calls.filter(c => c.path === '/last_message' && c.body.final_output === 'third').length > 0)
    failing = true
    await h.eventHandlers.turn_end({ message: { content: 'fourth', stopReason: 'stop' } })
    await waitFor(() => publishNotices().length === 2)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
}, 20000)

test('a new failure episode raises a fresh notice after the broker recovers', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  let failing = false
  let failures = 0
  const ownRun = () => String(b.calls.find(c => c.path === '/register')?.body.run_id ?? '')
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? '{}'))
    if (failing && new URL(String(url)).pathname === '/poll-control' && body.run_id === ownRun()) {
      failures++
      return Response.json({ code: 'internal' }, { status: 500 })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  const pollNotices = () => h!.notify.mock.calls
    .map((call: unknown[]) => String(call[0]))
    .filter(text => text.includes('central broker poll failed'))
  try {
    // A fast tick so three failures and a recovery fit inside the test's budget.
    h = await startExtensionHarness({
      deps: { messageTool },
      extOptions: { topLevelBrokerUrl: 'https://broker.test', pollIntervalMs: 100 },
    })
    failing = true
    await waitFor(() => pollNotices().length === 1)
    // The episode has to reach its floor of three failures before a clean tick
    // counts as recovery, so the broker coming back means nothing until then.
    await waitFor(() => failures >= 3, 10000)
    failing = false
    await waitFor(() => h!.notify.mock.calls.some(call => String(call[0]).includes('poll recovered')), 10000)
    failing = true
    await waitFor(() => pollNotices().length === 2, 10000)
    expect(pollNotices()).toHaveLength(2)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); await Bun.sleep(250); b.restore() }
}, 20000)

test('a retirement the broker never confirms is a notice, not raw stderr', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const originalError = console.error
  const errorLog = mock(() => {})
  console.error = errorLog as any
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/retire') throw new Error('broker gone')
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({ deps: { messageTool }, extOptions: { topLevelBrokerUrl: 'https://broker.test' } })
    await (await import('./test-fixtures.js')).waitFor(() => b.calls.some(c => c.path === '/register'))
    // The report runs after the session state is torn down, so it has to carry
    // the target it was told about rather than reading a host that no longer exists.
    await h.eventHandlers.session_shutdown({}, {})
    const notices = h.notify.mock.calls.map((call: unknown[]) => String(call[0]))
    expect(notices.some(text => text.includes('retirement could not be confirmed'))).toBe(true)
    expect(errorLog.mock.calls.map((call: unknown[]) => String(call[0])).join('\n'))
      .not.toContain('retirement could not be confirmed')
  } finally { console.error = originalError; b.restore() }
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
    // Delivery after a throwing first attempt is the property; the attempt count
    // is the retry loop's shape, and a reordering that still delivers is fine.
    await waitFor(() => delivered.length === 2)
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

test('last-turn retries preserve sequence and delayed older commits cannot replace the newer turn', async () => {
  const b = broker()
  const delegate = globalThis.fetch
  const attempts: Array<Record<string, unknown>> = []
  const state: { latest: Record<string, unknown> | null } = { latest: null }
  let highest = 0
  function commit(record: Record<string, unknown>) {
    if (Number(record.sequence) > highest) { highest = Number(record.sequence); state.latest = record }
  }
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    if (new URL(String(url)).pathname === '/last_message' && init?.method === 'POST') {
      const body = JSON.parse(String(init.body))
      attempts.push(body)
      if (attempts.length === 1) throw new Error('response lost; remote request still in flight')
      commit(body)
      return Response.json({ ok: true })
    }
    return delegate(url, init)
  }, { preconnect() {} }) as typeof fetch
  let h: Awaited<ReturnType<typeof startExtensionHarness>> | undefined
  try {
    h = await startExtensionHarness({
      deps: { Supervisor: class { static currentInstance() { return undefined } } as never },
      extOptions: { topLevelBrokerUrl: 'https://broker.test' },
    })
    await h.eventHandlers.turn_end({ message: { content: 'older turn', stopReason: 'stop' } })
    await waitFor(() => state.latest?.final_output === 'older turn')
    await h.eventHandlers.turn_end({ message: { content: 'newer turn', stopReason: 'stop' } })
    commit(attempts[0])
    expect(attempts.map(r => r.sequence)).toEqual([1, 1, 2])
    expect(state.latest?.final_output).toBe('newer turn')
    await h.eventHandlers.session_shutdown({}, {})
    await h.eventHandlers.session_start({}, h.ctx)
    highest = 0
    await h.eventHandlers.turn_end({ message: { content: 'new session', stopReason: 'stop' } })
    expect(attempts.at(-1)?.sequence).toBe(1)
    expect(attempts.at(-1)?.run_id).not.toBe(attempts[0].run_id)
  } finally { await h?.eventHandlers.session_shutdown({}, {}); b.restore() }
})

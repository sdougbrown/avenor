import { describe, expect, it, mock } from 'bun:test'
import {
  askViaBroker,
  formatInboundAskBody,
  parseControlMessage,
  postBroker,
  readSubAgentEnv,
  resolveReplyTarget,
} from './subagent-broker.js'
import { waitFor, waitUntilAborted } from './test-fixtures.js'

const env = { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_1', token: 'tok' } as const

describe('readSubAgentEnv', () => {
  it('returns null when any credential is missing', () => {
    expect(readSubAgentEnv({ AVENOR_BROKER_URL: 'http://x', AVENOR_RUN_ID: 'rt_1' })).toBeNull()
    expect(readSubAgentEnv({})).toBeNull()
  })

  it('treats a present-but-empty credential as missing', () => {
    expect(readSubAgentEnv({ AVENOR_BROKER_URL: 'http://x', AVENOR_RUN_ID: 'rt_1', AVENOR_BROKER_TOKEN: '' })).toBeNull()
  })

  it('reads the three broker credentials', () => {
    expect(readSubAgentEnv({
      AVENOR_BROKER_URL: 'http://x',
      AVENOR_RUN_ID: 'rt_1',
      AVENOR_BROKER_TOKEN: 'tok',
    })).toEqual({ brokerUrl: 'http://x', runId: 'rt_1', token: 'tok' })
  })
})

describe('parseControlMessage', () => {
  it('parses an agent_message with an object payload', () => {
    expect(parseControlMessage({
      type: 'agent_message',
      payload: { id: 'ask-1', from_run_id: 'supervisor', message: 'hello' },
    })).toEqual({ from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello' })
  })

  it('parses an agent_message with a string payload', () => {
    expect(parseControlMessage({
      type: 'agent_message',
      payload: JSON.stringify({ id: 'ask-2', from: 'supervisor', message: 'hi' }),
    })).toEqual({ from_run_id: 'supervisor', message_id: 'ask-2', message: 'hi' })
  })

  it('rejects non agent_message or malformed payloads', () => {
    expect(parseControlMessage({ type: 'done', payload: {} })).toBeNull()
    expect(parseControlMessage({ type: 'agent_message', payload: 'not-json' })).toBeNull()
    expect(parseControlMessage({ type: 'agent_message', payload: { message: '' } })).toBeNull()
    expect(parseControlMessage({ type: 'agent_message', payload: { message: 'x' } })).toBeNull()
    expect(parseControlMessage({ type: 'agent_message' })).toBeNull()
    // An ask with no asker could never be answered.
    expect(parseControlMessage({ type: 'agent_message', payload: { id: 'ask-9', message: 'x' } })).toBeNull()
  })
})

describe('formatInboundAskBody', () => {
  it('includes the asker and the reply instruction', () => {
    const body = formatInboundAskBody({ from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello' })
    expect(body).toContain('supervisor')
    expect(body).toContain('hello')
    expect(body).toContain('"ask-1"')
  })
})

describe('postBroker', () => {
  it('posts run_id/token plus the given body', async () => {
    const fetchMock = mock(async () => ({ ok: true, json: async () => ({ queued: true }) }))
    const out = await postBroker(env, '/send', { type: 'agent_message' }, fetchMock as unknown as typeof fetch)
    expect(out).toEqual({ queued: true })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('http://127.0.0.1:1/send')
    expect(JSON.parse(String(init.body))).toEqual({
      run_id: 'rt_1',
      token: 'tok',
      type: 'agent_message',
    })
  })

  it('throws on a non-ok response', async () => {
    const fetchMock = mock(async () => ({ ok: false, status: 404, text: async () => 'nope' }))
    await expect(postBroker(env, '/send', {}, fetchMock as unknown as typeof fetch)).rejects.toThrow('broker /send: 404 nope')
  })

  it('keeps the credentials when the body carries its own', async () => {
    const fetchMock = mock(async () => ({ ok: true, json: async () => ({}) }))
    await postBroker(env, '/send', { run_id: 'other', token: 'stolen' }, fetchMock as unknown as typeof fetch)
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    expect(JSON.parse(String(init.body))).toEqual({ run_id: 'rt_1', token: 'tok' })
  })
})

describe('resolveReplyTarget', () => {
  const pending = new Map<string, { from_run_id: string; message_id: string }>([
    ['ask-1', { from_run_id: 'supervisor', message_id: 'ask-1' }],
    ['ask-2', { from_run_id: 'supervisor', message_id: 'ask-2' }],
    ['ask-3', { from_run_id: 'other', message_id: 'ask-3' }],
  ])

  it('resolves an explicit message id with its asker', () => {
    expect(resolveReplyTarget(pending, undefined, 'ask-2')).toEqual({ from_run_id: 'supervisor', message_id: 'ask-2' })
  })

  it('lets an explicit message id take precedence over a disagreeing asker', () => {
    expect(resolveReplyTarget(pending, 'other', 'ask-1')).toEqual({ from_run_id: 'other', message_id: 'ask-1' })
  })

  it('falls back to the most recent pending ask for a given asker', () => {
    expect(resolveReplyTarget(pending, 'supervisor', undefined)).toEqual({ from_run_id: 'supervisor', message_id: 'ask-2' })
  })

  it('picks by insertion order, not lexicographic message-id order', () => {
    // Ask ids are random tokens; the lexically-larger id was inserted first.
    const ordered = new Map<string, { from_run_id: string; message_id: string }>([
      ['zz-older', { from_run_id: 'supervisor', message_id: 'zz-older' }],
      ['aa-newer', { from_run_id: 'supervisor', message_id: 'aa-newer' }],
    ])
    expect(resolveReplyTarget(ordered, 'supervisor', undefined)).toEqual({ from_run_id: 'supervisor', message_id: 'aa-newer' })
    expect(resolveReplyTarget(ordered, undefined, undefined)).toEqual({ from_run_id: 'supervisor', message_id: 'aa-newer' })
  })

  it('returns null when nothing matches', () => {
    expect(resolveReplyTarget(pending, 'ghost', undefined)).toBeNull()
    expect(resolveReplyTarget(new Map(), undefined, undefined)).toBeNull()
    // A reply_to_message_id that is not a pending ask must not be answered
    // fire-and-forget, with or without an asker.
    expect(resolveReplyTarget(pending, undefined, 'ask-unknown')).toBeNull()
    expect(resolveReplyTarget(pending, 'supervisor', 'ask-unknown')).toBeNull()
  })
})

interface RecordedCall {
  path: string
  body: Record<string, unknown>
}

/** Fetch stub that answers each broker path from a per-test handler table. */
function brokerFetch(
  handlers: Record<string, (init: RequestInit | undefined) => Promise<Response>>,
  calls: RecordedCall[] = [],
): typeof fetch {
  return (async (url: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(url)).pathname
    calls.push({ path, body: JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown> })
    const handler = handlers[path]
    if (!handler) throw new Error(`unexpected broker path ${path}`)
    return handler(init)
  }) as typeof fetch
}

const jsonResponse = (value: unknown, status = 200): Response =>
  new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })

describe('askViaBroker', () => {
  it('sends the ask as the child run and returns the correlated reply', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true, message_id: 'ignored', expects_reply: true }),
        '/wait_reply': async () => jsonResponse({ from_run_id: 'supervisor', payload: { message: 'yes, green' } }),
      },
      calls,
    )

    const result = await askViaBroker(env, { toRunId: 'supervisor', message: 'is the build green?' }, fetchImpl)
    expect(result).toEqual({ reply: 'yes, green', from_run_id: 'supervisor' })

    const send = calls.find(call => call.path === '/send')
    const wait = calls.find(call => call.path === '/wait_reply')
    expect(send?.body).toMatchObject({
      run_id: 'rt_1',
      token: 'tok',
      from_run_id: 'rt_1',
      to_run_id: 'supervisor',
      type: 'agent_message',
    })
    const payload = send?.body.payload as Record<string, unknown>
    expect(typeof payload.id).toBe('string')
    expect(payload).toEqual({
      id: payload.id,
      from: 'rt_1',
      from_run_id: 'rt_1',
      to_run_id: 'supervisor',
      message: 'is the build green?',
      role: 'agent',
      expects_reply: true,
    })
    expect(wait?.body).toMatchObject({ run_id: 'rt_1', token: 'tok', waiting_for: payload.id })
  })

  it('withdraws on timeout and reports the ask as withdrawn', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': waitUntilAborted,
        '/cancel_message': async () => jsonResponse({ cancelled: true }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello', timeoutMs: 10 }, fetchImpl)
      .catch((e: unknown) => e)

    const messageId = (calls.find(call => call.path === '/send')?.body.payload as Record<string, unknown>).id
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toContain(`message_id: ${String(messageId)}`)
    expect((error as Error).message).toContain('already withdrawn')
    expect((error as Error).message).toContain('wait for reply: timed out')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
    expect(calls.find(call => call.path === '/cancel_message')?.body).toMatchObject({
      run_id: 'rt_1',
      cancel_message_id: messageId,
    })
  })

  it('reports the ask as possibly pending when the withdraw hits a transport failure', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': async () => {
          throw new TypeError('fetch failed')
        },
        '/cancel_message': async () => {
          throw new TypeError('fetch failed')
        },
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    const messageId = String((calls.find(call => call.path === '/send')?.body.payload as Record<string, unknown>).id)
    expect((error as Error).message).toContain(`message_id: ${messageId}`)
    expect((error as Error).message).toContain('pass to avenor_cancel to withdraw')
    expect((error as Error).message).toContain('wait for reply: fetch failed')
  })

  it('reports the ask as withdrawn when the withdraw finds no pending ask', async () => {
    const fetchImpl = brokerFetch({
      '/send': async () => jsonResponse({ queued: true }),
      '/wait_reply': async () => jsonResponse({ message_id: 'x', timeout: true }, 504),
      '/cancel_message': async () => new Response('no pending ask for this message id', { status: 404 }),
    })

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('already withdrawn')
    expect((error as Error).message).toContain('wait for reply: broker /wait_reply: 504')
  })

  it('treats a non-404 withdraw failure as possibly pending', async () => {
    const fetchImpl = brokerFetch({
      '/send': async () => jsonResponse({ queued: true }),
      '/wait_reply': async () => jsonResponse({ message_id: 'x', timeout: true }, 504),
      '/cancel_message': async () => new Response('no pending ask for this message id', { status: 503 }),
    })

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('pass to avenor_cancel to withdraw')
  })

  it('withdraws when the caller aborts the ask', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': waitUntilAborted,
        '/cancel_message': async (init) => {
          // Cleanup runs on its own deadline: the caller's signal is already aborted.
          expect(init?.signal?.aborted).toBe(false)
          return new Response('no pending ask for this message id', { status: 404 })
        },
      },
      calls,
    )
    const controller = new AbortController()
    const pending = askViaBroker(env, { toRunId: 'supervisor', message: 'hello', signal: controller.signal }, fetchImpl)
    await waitFor(() => calls.some(call => call.path === '/wait_reply'))
    controller.abort()

    const error = await pending.catch((e: unknown) => e)
    expect((error as Error).message).toContain('wait for reply: cancelled')
    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it('withdraws when the caller aborts an ask that also carries a timeout', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': waitUntilAborted,
        '/cancel_message': async () => new Response('no pending ask for this message id', { status: 404 }),
      },
      calls,
    )
    const controller = new AbortController()
    const pending = askViaBroker(
      env,
      { toRunId: 'supervisor', message: 'hello', timeoutMs: 10_000, signal: controller.signal },
      fetchImpl,
    )
    await waitFor(() => calls.some(call => call.path === '/wait_reply'))
    controller.abort()

    const error = await pending.catch((e: unknown) => e)
    expect((error as Error).message).toContain('wait for reply: cancelled')
    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it('reports the deadline when the timeout fires before a present caller signal', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': waitUntilAborted,
        '/cancel_message': async () => new Response('no pending ask for this message id', { status: 404 }),
      },
      calls,
    )
    const controller = new AbortController()

    const error = await askViaBroker(
      env,
      { toRunId: 'supervisor', message: 'hello', timeoutMs: 10, signal: controller.signal },
      fetchImpl,
    ).catch((e: unknown) => e)

    expect(controller.signal.aborted).toBe(false)
    expect((error as Error).message).toContain('wait for reply: timed out')
    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it('withdraws when the caller aborts while the send is in flight', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': waitUntilAborted,
        '/cancel_message': async () => new Response('cancel failed', { status: 500 }),
      },
      calls,
    )
    const controller = new AbortController()
    const pending = askViaBroker(env, { toRunId: 'supervisor', message: 'hello', signal: controller.signal }, fetchImpl)
    await waitFor(() => calls.some(call => call.path === '/send'))
    controller.abort()

    const error = await pending.catch((e: unknown) => e)
    // The send may have registered an edge before aborting, so it still needs a
    // withdraw, and an unconfirmable withdraw leaves the ask possibly pending.
    expect((error as Error).message).toContain('send ask: cancelled')
    expect((error as Error).message).toContain('pass to avenor_cancel to withdraw')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it('withdraws an ask whose send drew an HTTP error response', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => new Response('to run not found', { status: 404 }),
        '/cancel_message': async () => new Response('no pending ask for this message id', { status: 404 }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'ghost', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('already withdrawn')
    expect((error as Error).message).toContain('send ask: broker /send: 404 to run not found')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it.each([
    ['a Node dial failure cause', () => new TypeError('fetch failed', {
      cause: Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' }),
    })],
    ['a Node connect timeout cause', () => new TypeError('fetch failed', {
      cause: Object.assign(new Error('Connect Timeout Error'), { code: 'UND_ERR_CONNECT_TIMEOUT' }),
    })],
    ['a Bun connection-refused code', () => Object.assign(new TypeError('Unable to connect.'), { code: 'ConnectionRefused' })],
    ['an unresolvable broker host', () => Object.assign(new TypeError('getaddrinfo ENOTFOUND'), { code: 'ENOTFOUND' })],
    ['an unusable broker address', () => Object.assign(new TypeError('fetch() URL is invalid'), { code: 'ERR_INVALID_URL' })],
  ])('reports an ask that never reached the broker as withdrawn on %s', async (_name, makeError) => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => {
          throw makeError()
        },
        '/cancel_message': async () => jsonResponse({ cancelled: true }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    const messageId = String((calls.find(call => call.path === '/send')?.body.payload as Record<string, unknown>).id)
    expect((error as Error).message).toContain(`message_id: ${messageId}`)
    expect((error as Error).message).toContain('already withdrawn')
    expect((error as Error).message).not.toContain('pass to avenor_cancel to withdraw')
    expect(calls.filter(call => call.path === '/health')).toHaveLength(0)
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(0)
  })

  it('withdraws an ask whose send connection was reset after it opened', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => {
          throw new TypeError('fetch failed', {
            cause: Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
          })
        },
        '/health': async () => new Response('ok'),
        '/cancel_message': async () => jsonResponse({ cancelled: true }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('send ask: fetch failed')
    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it('reports a reset send as withdrawn when the broker no longer answers', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => {
          throw new TypeError('fetch failed', {
            cause: Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
          })
        },
        '/health': async () => {
          throw new TypeError('fetch failed', {
            cause: Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' }),
          })
        },
        '/cancel_message': async () => jsonResponse({ cancelled: true }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('send ask: fetch failed')
    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/health')).toHaveLength(1)
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(0)
  })

  it('reports a reset send as withdrawn when the reachability probe times out', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => {
          throw new TypeError('fetch failed', {
            cause: Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
          })
        },
        '/health': waitUntilAborted,
        '/cancel_message': async () => jsonResponse({ cancelled: true }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(0)
  })

  it('withdraws when only some of several dial attempts failed at the dial', async () => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => {
          // Undici reports one error per dialled address; a reset on another
          // address may have carried the request.
          throw new TypeError('fetch failed', {
            cause: new AggregateError([
              Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { code: 'ECONNREFUSED' }),
              Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }),
            ], 'aggregate error'),
          })
        },
        '/health': async () => new Response('ok'),
        '/cancel_message': async () => new Response('no pending ask for this message id', { status: 404 }),
      },
      calls,
    )

    const error = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello' }, fetchImpl)
      .catch((e: unknown) => e)

    expect((error as Error).message).toContain('already withdrawn')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(1)
  })

  it.each([
    ['zero', 0],
    ['negative', -1],
    ['NaN', NaN],
    ['Infinity', Infinity],
  ])('ignores a %s timeoutMs and keeps the default role', async (_name, timeoutMs) => {
    const calls: RecordedCall[] = []
    const fetchImpl = brokerFetch(
      {
        '/send': async () => jsonResponse({ queued: true }),
        '/wait_reply': async () => jsonResponse({ from_run_id: 'supervisor', payload: { message: 'still here' } }),
      },
      calls,
    )

    const result = await askViaBroker(env, { toRunId: 'supervisor', message: 'hello', timeoutMs }, fetchImpl)
    expect(result).toEqual({ reply: 'still here', from_run_id: 'supervisor' })
    const payload = calls.find(call => call.path === '/send')?.body.payload as Record<string, unknown>
    expect(payload.role).toBe('agent')
    expect(calls.filter(call => call.path === '/cancel_message')).toHaveLength(0)
  })
})

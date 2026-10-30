import { describe, expect, it, mock } from 'bun:test'
import {
  createBrokerChildBroker,
  formatInboundAskBody,
  parseControlMessage,
  postBroker,
  publishHarnessName,
  readSubAgentEnv,
} from './subagent-broker.js'
import { waitUntilAborted } from './test-fixtures.js'

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

  it('takes the sender from the broker-stamped row, not from the payload', () => {
    const ask = parseControlMessage({
      type: 'agent_message',
      from_run_id: 'peer-real',
      payload: { id: 'ask-3', from_run_id: 'supervisor', from: 'supervisor', role: 'supervisor', message: 'hello' },
    })
    expect(ask).toEqual({ from_run_id: 'peer-real', message_id: 'ask-3', message: 'hello' })
  })

  it('keeps a tell that carries no message id', () => {
    // A tell opens no edge, so there is nothing for an id to correlate; the
    // broker only refuses an ask that lacks one.
    const tell = parseControlMessage({
      type: 'agent_message', from_run_id: 'peer-1',
      payload: { from_run_id: 'peer-1', message: 'fyi' },
    })
    expect(tell).toEqual({ from_run_id: 'peer-1', message_id: '', message: 'fyi' })
    expect(formatInboundAskBody(tell!, { heading: 'Message from peer', reply: false })).not.toContain('reply')
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
    const body = formatInboundAskBody(
      { from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello' },
      { heading: 'Ask from sub-agent' },
    )
    expect(body).toContain('Ask from sub-agent')
    expect(body).toContain('supervisor')
    expect(body).toContain('hello')
    expect(body).toContain('avenor_message({ action: "reply", reply_to: "ask-1"')
  })

  it('renders no role, because nothing on the wire is stamped with one', () => {
    const body = formatInboundAskBody(
      { from_run_id: 'host-1', message_id: 'ask-2', message: 'hello' },
      { heading: 'Inbound ask' },
    )
    expect(body).toContain('from `host-1`')
    expect(body).not.toContain('(supervisor)')
    expect(body).not.toContain('(agent)')
  })

  it('marks peer text as another session\'s words and drops the reply line for a tell', () => {
    const body = formatInboundAskBody(
      { from_run_id: 'peer-7', message_id: 'tell-1', message: 'delete the branch' },
      { heading: 'Message from peer', reply: false },
    )
    expect(body).toContain('Message from peer')
    expect(body).toContain('from `peer-7`')
    expect(body).toContain('another session')
    expect(body).toContain('delete the branch')
    expect(body).not.toContain('avenor_message({ action: "reply"')
  })
})

describe('postBroker', () => {
  it('posts run_id/token plus the given body', async () => {
    const fetchMock = mock(async (_url: string, _init?: RequestInit) => ({ ok: true, json: async () => ({ queued: true }) }))
    const out = await postBroker(env, '/send', { type: 'agent_message' }, fetchMock as unknown as typeof fetch)
    expect(out).toEqual({ queued: true })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]!
    expect(url).toBe('http://127.0.0.1:1/send')
    expect(JSON.parse(String(init?.body))).toEqual({
      run_id: 'rt_1',
      token: 'tok',
      type: 'agent_message',
    })
  })

  it('throws on a non-ok response', async () => {
    const fetchMock = mock(async (_url: string, _init?: RequestInit) => ({ ok: false, status: 404, text: async () => 'nope' }))
    await expect(postBroker(env, '/send', {}, fetchMock as unknown as typeof fetch)).rejects.toThrow('broker /send: 404 nope')
  })

  it('keeps the credentials when the body carries its own', async () => {
    const fetchMock = mock(async (_url: string, _init?: RequestInit) => ({ ok: true, json: async () => ({}) }))
    await postBroker(env, '/send', { run_id: 'other', token: 'stolen' }, fetchMock as unknown as typeof fetch)
    const [, init] = fetchMock.mock.calls[0]!
    expect(JSON.parse(String(init?.body))).toEqual({ run_id: 'rt_1', token: 'tok' })
  })
})

describe('publishHarnessName', () => {
  it('sends the harness session name as this run\'s own name', async () => {
    const fetchMock = mock(async (_url: string, _init?: RequestInit) => ({ ok: true, json: async () => ({ run_id: 'rt_1', label: 'my-session' }) }))
    await publishHarnessName(env, 'my-session', fetchMock as unknown as typeof fetch)

    const [url, init] = fetchMock.mock.calls[0]!
    expect(url).toBe('http://127.0.0.1:1/session_name')
    expect(JSON.parse(String(init?.body))).toEqual({ name: 'my-session', run_id: 'rt_1', token: 'tok' })
  })

  it('clears the name the harness no longer has', async () => {
    const fetchMock = mock(async (_url: string, _init?: RequestInit) => ({ ok: true, json: async () => ({}) }))
    await publishHarnessName(env, undefined, fetchMock as unknown as typeof fetch)

    const [, init] = fetchMock.mock.calls[0]!
    expect(JSON.parse(String(init?.body))).toEqual({ name: '', run_id: 'rt_1', token: 'tok' })
  })
})

type FetchCall = { url: string, init?: RequestInit, body: Record<string, unknown> }

/** Stub fetch per path and record every request. */
function stubFetch(handlers: Record<string, (init: RequestInit | undefined) => Response | Promise<Response>>) {
  const calls: FetchCall[] = []
  const fetchImpl = (async (url: string | URL | Request, init?: RequestInit) => {
    const href = String(url)
    const path = new URL(href).pathname + new URL(href).search
    calls.push({ url: href, init, body: JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown> })
    const handler = handlers[path.split('?')[0]]
    if (!handler) {
      // The broker always answers /sessions; a child pins its generation there.
      if (path === '/sessions') {
        return Response.json({ sessions: [], generation: 'gen-1', messaging_protocol: 1 })
      }
      return new Response('not found', { status: 404 })
    }
    return handler(init)
  }) as typeof fetch
  return { calls, fetchImpl }
}

describe('child broker ask', () => {
  it('sends the ask as the child run and returns the correlated reply', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
      '/wait_reply': () => Response.json({ outcome: 'replied', from_run_id: 'supervisor', payload: { message: 'green' } }),
    })
    const broker = createBrokerChildBroker(env, fetchImpl)
    const result = await broker.ask('supervisor', 'is it green?', 'av1.m.1')
    expect(result).toEqual({ outcome: 'replied', messageId: 'av1.m.1', reply: 'green' })
    const send = calls.find(call => call.url.endsWith('/send'))
    expect(send?.body).toMatchObject({ run_id: 'rt_1', token: 'tok', from_run_id: 'rt_1', to_run_id: 'supervisor', type: 'agent_message' })
    const payload = send?.body.payload as Record<string, unknown>
    expect(payload).toMatchObject({ id: 'av1.m.1', expects_reply: true, message: 'is it green?' })
    expect(calls.find(call => call.url.endsWith('/wait_reply'))?.body).toMatchObject({
      run_id: 'rt_1',
      token: 'tok',
      waiting_for: 'av1.m.1',
    })
  })

  it('reports a 200 withdrawn wait as withdrawn without re-cancelling the ask', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true }),
      '/wait_reply': () => Response.json({ cancelled: true, message_id: 'av1.m.2', outcome: 'withdrawn' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.2', 1)
    expect(result.outcome).toBe('withdrawn')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('reports a broker-side wait timeout as timed-out, not withdrawn', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true }),
      '/wait_reply': () => Response.json({ timeout: true, outcome: 'timed-out' }, { status: 504 }),
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.9')
    expect(result.outcome).toBe('timed-out')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(true)
  })

  it('lets its own deadline time a pending wait out and cancels the ask', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true }),
      '/wait_reply': init => waitUntilAborted(init),
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.10', 5)
    expect(result.outcome).toBe('timed-out')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(true)
  })

  it('reports a send whose dial never opened as not-sent without spending a cancel', async () => {
    const dialError = Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } })
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Promise.reject(dialError),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.3')
    expect(result.outcome).toBe('not-sent')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('reports a rejected ask-send as the rejection instead of the withdrawal', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ code: 'target-unavailable' }, { status: 404 }),
      '/cancel_message': () => new Response('no pending ask', { status: 404 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('rt_gone', 'q', 'av1.m.10')
    expect(result).toEqual({ outcome: 'not-sent', messageId: 'av1.m.10' })
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('reports an unenveloped ask-send rejection as possibly-pending without spending a cancel', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => new Response('boom', { status: 500 }),
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.4')
    expect(result.outcome).toBe('possibly-pending')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('reports possibly-pending when the cleanup cannot be confirmed', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
      '/wait_reply': () => Promise.reject(new Error('connection reset')),
      '/cancel_message': () => new Response('unavailable', { status: 503 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.5')
    expect(result.outcome).toBe('possibly-pending')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(true)
  })

  it('reports a wait failure whose cleanup confirmed the edge gone as edge-absent', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
      '/wait_reply': () => Promise.reject(new Error('connection reset')),
      '/cancel_message': () => new Response('no pending ask', { status: 404 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.11')
    expect(result).toEqual({ outcome: 'edge-absent', messageId: 'av1.m.11' })
  })

  it('withdraws a child ask when the caller aborts', async () => {
    const controller = new AbortController()
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true }),
      '/wait_reply': init => new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal
        if (!signal) return reject(new Error('the wait must carry the caller signal'))
        if (signal.aborted) return reject(signal.reason)
        signal.addEventListener('abort', () => reject(signal.reason), { once: true })
      }),
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    const pending = createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.6', undefined, controller.signal)
    await new Promise(resolve => setTimeout(resolve, 5))
    controller.abort()
    const result = await pending
    expect(result.outcome).toBe('withdrawn')
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(true)
  })
})

describe('child broker routes', () => {
  it('reports a rejected child tell as the wire outcome instead of a refusal', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => Response.json({ code: 'target-unavailable' }, { status: 404 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).tell('rt_gone', 'hi', 'av1.t.1')
    expect(result).toEqual({ outcome: 'not-sent', messageId: 'av1.t.1' })
  })

  it('reports a rejected child reply as the wire outcome instead of a refusal', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => Response.json({ code: 'not-ask-target', outcome: 'edge-absent' }, { status: 403 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).reply('rt_asker', 'av1.r.1', 'OK')
    expect(result).toEqual({ outcome: 'edge-absent', messageId: 'av1.r.1' })
  })

  it('replies under the inbound ask id and carries reply_to without a generation', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).reply('rt_asker', 'ask-9', 'green')
    expect(result).toEqual({ outcome: 'sent', messageId: 'ask-9' })
    const send = calls.find(call => call.url.endsWith('/send'))
    expect(send?.body.generation).toBeUndefined()
    const payload = send?.body.payload as Record<string, unknown>
    expect(payload).toMatchObject({
      id: 'ask-9.reply',
      from: 'rt_1',
      from_run_id: 'rt_1',
      to_run_id: 'rt_asker',
      role: 'agent',
      reply_to: 'ask-9',
      message: 'green',
    })
    expect(payload).not.toHaveProperty('expects_reply')
    expect(calls.some(call => call.url.endsWith('/sessions'))).toBe(false)
  })

  it('sends a reply to a tell with no to_run_id, leaving the broker to resolve the sender', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).reply(undefined, 'tell-9', 'ack')
    expect(result).toEqual({ outcome: 'sent', messageId: 'tell-9' })
    const send = calls.find(call => call.url.endsWith('/send'))
    expect(send?.body).not.toHaveProperty('to_run_id')
    const payload = send?.body.payload as Record<string, unknown>
    expect(payload).toMatchObject({ id: 'tell-9.reply', reply_to: 'tell-9', message: 'ack' })
    expect(payload).not.toHaveProperty('to_run_id')
  })

  it('classifies a rejection the envelope does not describe by its status', async () => {
    const boom = stubFetch({ '/send': () => new Response('boom', { status: 500 }) })
    expect(await createBrokerChildBroker(env, boom.fetchImpl).tell('rt_2', 'hi', 'av1.t.2')).toEqual({
      outcome: 'possibly-pending',
      messageId: 'av1.t.2',
    })

    const denied = stubFetch({ '/send': () => new Response('nope', { status: 401 }) })
    expect((await createBrokerChildBroker(env, denied.fetchImpl).tell('rt_2', 'hi', 'av1.t.3')).outcome).toBe('auth-rejected')

    const forbidden = stubFetch({ '/send': () => new Response('not the ask target', { status: 403 }) })
    expect((await createBrokerChildBroker(env, forbidden.fetchImpl).reply('rt_2', 'av1.r.2', 'OK')).outcome).toBe('edge-absent')
  })

  it('reports an unknown wire code as possibly-pending', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => Response.json({ code: 'mystery' }, { status: 500 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).tell('rt_2', 'hi', 'av1.t.4')
    expect(result.outcome).toBe('possibly-pending')
  })

  it('lets an enveloped outcome win over the rejection status', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => Response.json({ outcome: 'auth-rejected' }, { status: 500 }),
    })
    expect((await createBrokerChildBroker(env, fetchImpl).tell('rt_2', 'hi', 'av1.t.5')).outcome).toBe('auth-rejected')
  })

  it('maps an absent withdrawal to edge-absent and a full fence to possibly-pending', async () => {
    const absent = stubFetch({
      '/cancel_message': () => Response.json(
        { error: 'no pending ask for this message id', outcome: 'edge-absent' },
        { status: 404 },
      ),
    })
    expect(await createBrokerChildBroker(env, absent.fetchImpl).withdraw('av1.m.7')).toEqual({
      outcome: 'edge-absent',
    })
    const full = stubFetch({
      '/cancel_message': () => Response.json({ code: 'fence-capacity' }, { status: 503 }),
    })
    expect((await createBrokerChildBroker(env, full.fetchImpl).withdraw('av1.m.8')).outcome).toBe('possibly-pending')
  })

  it('maps a code-less refusal by its status when the body carries no wire code', async () => {
    const forbidden = stubFetch({ '/last_message': () => new Response('forbidden', { status: 403 }) })
    await expect(createBrokerChildBroker(env, forbidden.fetchImpl).last('ghost'))
      .rejects.toMatchObject({ name: 'ToolError', code: 'not_permitted', message: 'forbidden' })

    const conflict = stubFetch({ '/rename': () => new Response('two matches', { status: 409 }) })
    await expect(createBrokerChildBroker(env, conflict.fetchImpl).rename({ name: 'dup' }))
      .rejects.toMatchObject({ name: 'ToolError', code: 'ambiguous_name', message: 'two matches' })
  })

  it('rejects a withdrawal whose cancel drew no HTTP response', async () => {
    const { fetchImpl } = stubFetch({
      '/cancel_message': () => Promise.reject(new Error('socket hang up')),
    })
    await expect(createBrokerChildBroker(env, fetchImpl).withdraw('av1.m.40'))
      .rejects.toThrow('socket hang up')
  })

  it('lists peers from /sessions with header credentials', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/sessions': () => Response.json({
        sessions: [{ run_id: 'supervisor', label: 'main', kind: 'supervisor', status: 'running' }],
      }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).peers()
    expect(result.peers).toEqual([
      { run_id: 'supervisor', label: 'main', kind: 'supervisor', status: 'running' },
    ])
    const headers = calls[0].init?.headers as Record<string, string>
    expect(headers['X-Avenor-Run-Id']).toBe('rt_1')
    expect(headers['X-Avenor-Run-Token']).toBe('tok')
  })

  it('carries a partial listing error from /sessions as the peers detail', async () => {
    const { fetchImpl } = stubFetch({
      '/sessions': () => Response.json({
        sessions: [{ run_id: 'supervisor', label: 'main' }],
        peers_error: 'peer sparky has an unreadable session record: unexpected end of JSON input',
      }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).peers()
    expect(result.peers).toEqual([{ run_id: 'supervisor', label: 'main' }])
    expect(result.detail).toBe('peer sparky has an unreadable session record: unexpected end of JSON input')
  })

  it('leaves a complete listing without a peers detail', async () => {
    const { fetchImpl } = stubFetch({
      '/sessions': () => Response.json({
        sessions: [{ run_id: 'supervisor', label: 'main' }],
        peers_error: '',
      }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).peers()
    expect(result.peers).toEqual([{ run_id: 'supervisor', label: 'main' }])
    expect(result).toEqual({ peers: [{ run_id: 'supervisor', label: 'main' }] })
    expect('detail' in result).toBe(false)
  })

  it('keeps the peers a partial listing read and names the incompleteness', async () => {
    const { fetchImpl } = stubFetch({
      '/sessions': () => Response.json({
        sessions: [{ run_id: 'supervisor', label: 'main', kind: 'supervisor', status: 'running' }],
        peers_error: 'the broker could not read every peer',
      }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).peers()
    expect(result.peers.map(peer => peer.run_id)).toEqual(['supervisor'])
    expect(result.detail).toBe('the broker could not read every peer')
  })

  it('renames through the authenticated /rename path', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/rename': () => Response.json({ run_id: 'rt_1', label: 'renamed', previous_label: 'old' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).rename({ name: 'renamed' })
    expect(result).toMatchObject({ runId: 'rt_1', label: 'renamed', previousLabel: 'old' })
    expect(calls[0].body).toMatchObject({ session_id: '', name: 'renamed' })
  })

  it('reads the last message and maps an unavailable target to unknown_target', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/last_message': () => Response.json({
        run_id: 'rt_2',
        label: 'peer',
        has_record: true,
        final_output: 'OK',
        stop_reason: 'end_turn',
      }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).last('rt_2')
    expect(result).toMatchObject({ label: 'peer', hasRecord: true, finalOutput: 'OK', stopReason: 'end_turn' })
    expect(calls[0].url).toContain('/last_message?target=rt_2')

    const missing = stubFetch({
      '/last_message': () => Response.json({ code: 'target-unavailable', error: 'no such target' }, { status: 404 }),
    })
    await expect(createBrokerChildBroker(env, missing.fetchImpl).last('ghost'))
      .rejects.toMatchObject({ code: 'unknown_target' })
  })
})

describe('child broker generation pin', () => {
  const generation = 'e7fd9f123bd148ba1f8f43f340197ef7'

  /**
   * A fake broker that refuses a versioned message id unless the request carries
   * the generation it advertises on /sessions, the way the ask store refuses one
   * that arrives without it.
   */
  function generationBroker(options: { advertised?: string, waitOutcome?: 'replied' | 'timed-out' } = {}) {
    const advertised = options.advertised ?? generation
    const calls: FetchCall[] = []
    const fetchImpl = (async (url: string | URL | Request, init?: RequestInit) => {
      const href = String(url)
      const path = new URL(href).pathname
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
      calls.push({ url: href, init, body })
      if (path === '/sessions') {
        return Response.json({
          sessions: [{ run_id: 'supervisor', label: 'main', kind: 'supervisor', status: 'running' }],
          generation: advertised,
          messaging_protocol: 1,
        })
      }
      if (body.generation !== generation) {
        return Response.json(
          { code: 'generation-mismatch', error: 'generation mismatch', outcome: 'not-sent' },
          { status: 409 },
        )
      }
      if (path === '/wait_reply') {
        return options.waitOutcome === 'timed-out'
          ? Response.json({ timeout: true, outcome: 'timed-out' }, { status: 504 })
          : Response.json({ outcome: 'replied', from_run_id: 'supervisor', payload: { message: 'pong' } })
      }
      if (path === '/cancel_message') return Response.json({ outcome: 'withdrawn' })
      return Response.json({ queued: true, outcome: 'sent' })
    }) as typeof fetch
    return { calls, fetchImpl }
  }

  it('pins the generation once and stamps it on the ask send and its wait', async () => {
    const { calls, fetchImpl } = generationBroker()
    const broker = createBrokerChildBroker(env, fetchImpl)
    expect(await broker.ask('supervisor', 'is it green?', 'av1.m.20')).toEqual({
      outcome: 'replied',
      messageId: 'av1.m.20',
      reply: 'pong',
    })
    expect((await broker.ask('supervisor', 'still green?', 'av1.m.21')).outcome).toBe('replied')

    expect(calls.filter(call => call.url.endsWith('/sessions'))).toHaveLength(1)
    expect(calls.filter(call => call.url.endsWith('/send')).map(call => call.body.generation))
      .toEqual([generation, generation])
    expect(calls.filter(call => call.url.endsWith('/wait_reply')).map(call => call.body.generation))
      .toEqual([generation, generation])
  })

  it('stamps the generation on a tell and on every ask withdrawal', async () => {
    const { calls, fetchImpl } = generationBroker({ waitOutcome: 'timed-out' })
    const broker = createBrokerChildBroker(env, fetchImpl)
    expect((await broker.tell('supervisor', 'hi', 'av1.t.20')).outcome).toBe('sent')
    expect((await broker.ask('supervisor', 'q', 'av1.m.22', 1)).outcome).toBe('timed-out')
    expect((await broker.withdraw('av1.m.23')).outcome).toBe('withdrawn')

    expect(calls.find(call => call.url.endsWith('/send'))?.body.generation).toBe(generation)
    expect(calls.filter(call => call.url.endsWith('/cancel_message')).map(call => call.body.generation))
      .toEqual([generation, generation])
  })

  it('reports an ask whose generation pin was refused as auth-rejected without sending', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/sessions': () => new Response('denied', { status: 401 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.30')
    expect(result).toEqual({ outcome: 'auth-rejected', messageId: 'av1.m.30' })
    expect(calls.some(call => call.url.endsWith('/send'))).toBe(false)
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('reports an ask whose generation pin failed as not-sent without sending', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/sessions': () => new Response('boom', { status: 500 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.31')
    expect(result).toEqual({ outcome: 'not-sent', messageId: 'av1.m.31' })
    expect(calls.some(call => call.url.endsWith('/send'))).toBe(false)
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('rejects a withdrawal whose generation pin was refused, without cancelling', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/sessions': () => new Response('denied', { status: 401 }),
    })
    await expect(createBrokerChildBroker(env, fetchImpl).withdraw('av1.m.32'))
      .rejects.toMatchObject({ name: 'ToolError', code: 'refused', message: 'denied' })
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })

  it('re-reads a generation the broker did not advertise instead of latching the empty pin', async () => {
    let reads = 0
    const { calls, fetchImpl } = stubFetch({
      '/sessions': () => {
        reads += 1
        return Response.json({ sessions: [], generation: reads === 1 ? '' : 'gen-2' })
      },
      '/send': () => Response.json({ queued: true, outcome: 'sent' }),
      '/wait_reply': () => Response.json({ outcome: 'replied', payload: { message: 'pong' } }),
    })
    const broker = createBrokerChildBroker(env, fetchImpl)
    expect((await broker.ask('supervisor', 'first', 'av1.m.33')).outcome).toBe('replied')
    expect((await broker.ask('supervisor', 'second', 'av1.m.34')).outcome).toBe('replied')
    expect(calls.filter(call => call.url.endsWith('/sessions'))).toHaveLength(2)
    expect(calls.filter(call => call.url.endsWith('/send')).map(call => call.body.generation))
      .toEqual(['', 'gen-2'])
  })

  it('reports an ask as not-sent when the broker advertises no generation', async () => {
    const { calls, fetchImpl } = generationBroker({ advertised: '' })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.24')
    expect(result).toEqual({ outcome: 'not-sent', messageId: 'av1.m.24' })
    expect(calls.filter(call => call.url.endsWith('/sessions'))).toHaveLength(1)
    expect(calls.some(call => call.url.endsWith('/cancel_message'))).toBe(false)
  })
})

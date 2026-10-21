import { describe, expect, it, mock } from 'bun:test'
import {
  createBrokerChildBroker,
  formatInboundAskBody,
  parseControlMessage,
  postBroker,
  readSubAgentEnv,
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
    const body = formatInboundAskBody(
      { from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello' },
      { heading: 'Ask from sub-agent', role: 'agent' },
    )
    expect(body).toContain('Ask from sub-agent')
    expect(body).toContain('supervisor')
    expect(body).toContain('hello')
    expect(body).toContain('avenor_message({ action: "reply", reply_to: "ask-1"')
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

type FetchCall = { url: string, init?: RequestInit, body: Record<string, unknown> }

/** Stub fetch per path and record every request. */
function stubFetch(handlers: Record<string, (init: RequestInit | undefined) => Response | Promise<Response>>) {
  const calls: FetchCall[] = []
  const fetchImpl = (async (url: string | URL | Request, init?: RequestInit) => {
    const href = String(url)
    const path = new URL(href).pathname + new URL(href).search
    calls.push({ url: href, init, body: JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown> })
    const handler = handlers[path.split('?')[0]]
    if (!handler) return new Response('not found', { status: 404 })
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

  it('reports a wait timeout as timed-out and cleans up the ask', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => Response.json({ queued: true }),
      '/wait_reply': () => Response.json({ outcome: 'timed-out' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.2', 1)
    expect(result.outcome).toBe('timed-out')
    expect(calls.some(call => call.url.endsWith('/wait_reply'))).toBe(true)
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

  it('withdraws an ask whose send drew an HTTP error response', async () => {
    const { calls, fetchImpl } = stubFetch({
      '/send': () => new Response('boom', { status: 500 }),
      '/cancel_message': () => Response.json({ outcome: 'withdrawn' }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.4')
    expect(result.outcome).toBe('withdrawn')
    expect(calls.find(call => call.url.endsWith('/cancel_message'))?.body).toMatchObject({ cancel_message_id: 'av1.m.4' })
  })

  it('reports possibly-pending when the cleanup cannot be confirmed', async () => {
    const { fetchImpl } = stubFetch({
      '/send': () => new Response('boom', { status: 500 }),
      '/cancel_message': () => new Response('unavailable', { status: 503 }),
    })
    const result = await createBrokerChildBroker(env, fetchImpl).ask('supervisor', 'q', 'av1.m.5')
    expect(result.outcome).toBe('possibly-pending')
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
  it('maps an absent withdrawal to edge-absent and a full fence to possibly-pending', async () => {
    const absent = stubFetch({
      '/cancel_message': () => new Response('no pending ask', { status: 404 }),
    })
    expect(await createBrokerChildBroker(env, absent.fetchImpl).withdraw('av1.m.7')).toEqual({
      outcome: 'edge-absent',
    })
    const full = stubFetch({
      '/cancel_message': () => Response.json({ code: 'fence-capacity' }, { status: 503 }),
    })
    expect((await createBrokerChildBroker(env, full.fetchImpl).withdraw('av1.m.8')).outcome).toBe('possibly-pending')
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

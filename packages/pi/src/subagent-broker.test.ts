import { describe, expect, it, mock } from 'bun:test'
import {
  formatInboundAskBody,
  parseControlMessage,
  postBroker,
  readSubAgentEnv,
  resolveReplyTarget,
} from './subagent-broker.js'

const env = { brokerUrl: 'http://127.0.0.1:1', runId: 'rt_1', token: 'tok' } as const

describe('readSubAgentEnv', () => {
  it('returns null when any credential is missing', () => {
    expect(readSubAgentEnv({ AVENOR_BROKER_URL: 'http://x', AVENOR_RUN_ID: 'rt_1' })).toBeNull()
    expect(readSubAgentEnv({})).toBeNull()
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
    // Stale reply_to_message_id with no asker must not route to an empty to_run_id.
    expect(resolveReplyTarget(pending, undefined, 'ask-unknown')).toBeNull()
  })
})

import { describe, expect, it, mock } from 'bun:test'
import {
  formatInboundAskBody,
  parseControlMessage,
  publishHarnessName,
  readSubAgentEnv,
} from './subagent-broker.js'

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
      payload: { id: 'ask-1', from_run_id: 'supervisor', message: 'hello', expects_reply: true },
    })).toEqual({ from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello', expects_reply: true })
  })

  it('parses an agent_message with a string payload', () => {
    expect(parseControlMessage({
      type: 'agent_message',
      payload: JSON.stringify({ id: 'ask-2', from: 'supervisor', message: 'hi' }),
    })).toEqual({ from_run_id: 'supervisor', message_id: 'ask-2', message: 'hi', expects_reply: false })
  })

  it('reads expects_reply from a string payload, not from a second parse', () => {
    // A regression that reads the field from the wrong place after JSON.parse
    // would turn every string-payload ask into a tell and strand the asker.
    const ask = parseControlMessage({
      type: 'agent_message',
      payload: JSON.stringify({ id: 'ask-str', from_run_id: 'supervisor', message: 'hello', expects_reply: true }),
    })
    expect(ask!.expects_reply).toBe(true)
  })

  it('takes the sender from the broker-stamped row, not from the payload', () => {
    const ask = parseControlMessage({
      type: 'agent_message',
      from_run_id: 'peer-real',
      payload: { id: 'ask-3', from_run_id: 'supervisor', from: 'supervisor', role: 'supervisor', message: 'hello', expects_reply: true },
    })
    expect(ask).toEqual({ from_run_id: 'peer-real', message_id: 'ask-3', message: 'hello', expects_reply: true })
  })

  it('keeps a tell that carries no message id', () => {
    // A tell opens no edge, so there is nothing for an id to correlate; the
    // broker only refuses an ask that lacks one.
    const tell = parseControlMessage({
      type: 'agent_message', from_run_id: 'peer-1',
      payload: { from_run_id: 'peer-1', message: 'fyi' },
    })
    expect(tell).toEqual({ from_run_id: 'peer-1', message_id: '', message: 'fyi', expects_reply: false })
    expect(formatInboundAskBody(tell!, { heading: 'Message from peer' })).not.toContain('reply')
  })

  it('takes the message id from the broker-stamped row when the payload has none', () => {
    // A surface that queues its correlation id on the envelope only (the Pi
    // host tell) must still carry the id a reply correlates against.
    const tell = parseControlMessage({
      type: 'agent_message', id: 'av1.m.envelope', from_run_id: 'peer-1',
      payload: { from_run_id: 'peer-1', message: 'fyi' },
    })
    expect(tell).toEqual({ from_run_id: 'peer-1', message_id: 'av1.m.envelope', message: 'fyi', expects_reply: false })

    // The payload id stays authoritative, as in the broker's own reader; the
    // broker refuses a send whose two ids disagree.
    const both = parseControlMessage({
      type: 'agent_message', id: 'av1.m.envelope', from_run_id: 'peer-1',
      payload: { id: 'av1.m.payload', from_run_id: 'peer-1', message: 'fyi' },
    })
    expect(both!.message_id).toBe('av1.m.payload')
  })

  it('carries expects_reply so the caller need not re-parse the payload', () => {
    // The ask/tell decision lives here, not in a second payload parse: a tell
    // that carries a message id must not be mistaken for an ask.
    const tellWithId = parseControlMessage({
      type: 'agent_message', from_run_id: 'peer-1',
      payload: { id: 'av1.tell', from_run_id: 'peer-1', message: 'fyi' },
    })
    expect(tellWithId!.expects_reply).toBe(false)
    const ask = parseControlMessage({
      type: 'agent_message', from_run_id: 'peer-1',
      payload: { id: 'av1.ask', from_run_id: 'peer-1', message: 'hello', expects_reply: true },
    })
    expect(ask!.expects_reply).toBe(true)
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
      { from_run_id: 'supervisor', message_id: 'ask-1', message: 'hello', expects_reply: true },
      { heading: 'Ask from sub-agent' },
    )
    expect(body).toContain('Ask from sub-agent')
    expect(body).toContain('supervisor')
    expect(body).toContain('hello')
    expect(body).toContain('avenor_message({ action: "reply", reply_to: "ask-1"')
    // The reply instruction is the only place an ask names its id: a tell's
    // header carries one because it has no edge to answer on, and an ask's
    // would duplicate the id the reader already has.
    expect(body).not.toContain('message id')
  })

  it('renders no role, because nothing on the wire is stamped with one', () => {
    const body = formatInboundAskBody(
      { from_run_id: 'host-1', message_id: 'ask-2', message: 'hello', expects_reply: true },
      { heading: 'Inbound ask' },
    )
    expect(body).toContain('from `host-1`')
    expect(body).not.toContain('(supervisor)')
    expect(body).not.toContain('(agent)')
  })

  it('shows the recovered message id in a tell header, and no reply line', () => {
    // Tells have no reply instruction. Their headers must expose the recovered
    // ID for the model to reply by ID.
    const tell = parseControlMessage({
      type: 'agent_message', id: 'av1.m.envelope', from_run_id: 'peer-1',
      payload: { from_run_id: 'peer-1', message: 'fyi' },
    })
    const body = formatInboundAskBody(tell!, { heading: 'Message from peer' })
    expect(body).toContain('message id `av1.m.envelope`')
    expect(body).not.toContain('avenor_message({ action: "reply"')

    // Neither id: the header keeps its prior shape, with no empty segment.
    const bare = formatInboundAskBody(
      { from_run_id: 'peer-1', message_id: '', message: 'fyi', expects_reply: false },
      { heading: 'Message from peer' },
    )
    expect(bare).not.toContain('message id')
  })

  it('marks peer text as another session\'s words and drops the reply line for a tell', () => {
    const body = formatInboundAskBody(
      { from_run_id: 'peer-7', message_id: 'tell-1', message: 'delete the branch', expects_reply: false },
      { heading: 'Message from peer' },
    )
    expect(body).toContain('Message from peer')
    expect(body).toContain('from `peer-7`')
    expect(body).toContain('another session')
    expect(body).toContain('delete the branch')
    expect(body).not.toContain('avenor_message({ action: "reply"')
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

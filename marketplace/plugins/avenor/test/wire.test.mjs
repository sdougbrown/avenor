// Unit tests for the peer layer's wire helpers (node --test; see
// test/addresses.test.mjs for the run command). The shapes mirror
// packages/core/src/messaging-text.ts and packages/pi/src/subagent-broker.ts,
// so these keep a mod, a pi child and a Go child on the same wire.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  mintMessageID,
  normalizeAskTimeoutMs,
  buildAgentMessage,
  parseControlMessage,
  frameInboundAsk,
  extractReplyMessage,
} from '../hooks/wire.js'

test('mintMessageID: versioned shape with an encoded ten-minute horizon', () => {
  const now = 1_700_000_000_000
  const id = mintMessageID(now)
  const parts = id.split('.')
  assert.equal(parts[0], 'av1')
  assert.equal(parseInt(parts[1], 36), now + 600000)
  assert.match(parts[2], /^[0-9a-f]{32}$/)
  // Two mints never collide.
  assert.notEqual(id, mintMessageID(now))
})

test('normalizeAskTimeoutMs: bounds to what the broker accepts', () => {
  assert.equal(normalizeAskTimeoutMs(undefined), 600000)
  assert.equal(normalizeAskTimeoutMs(0), 600000)
  assert.equal(normalizeAskTimeoutMs(-5), 600000)
  assert.equal(normalizeAskTimeoutMs(Number.NaN), 600000)
  assert.equal(normalizeAskTimeoutMs(1.9), 1)
  assert.equal(normalizeAskTimeoutMs(3000), 3000)
  assert.equal(normalizeAskTimeoutMs(600000), 600000)
  assert.equal(normalizeAskTimeoutMs(600001), 600000)
})

test('buildAgentMessage: tell, ask and reply envelopes', () => {
  const tell = buildAgentMessage({
    fromRunId: 'me', toRunId: 'peer', messageId: 't1', message: 'hi', role: 'agent',
  })
  assert.equal(tell.type, 'agent_message')
  assert.equal(tell.payload.expects_reply, undefined)
  assert.equal(tell.payload.reply_to, undefined)
  assert.equal(tell.payload.from_run_id, 'me')

  const ask = buildAgentMessage({
    fromRunId: 'me', toRunId: 'peer', messageId: 'a1', message: 'q', role: 'agent',
    expectsReply: true, generation: 'gen-1',
  })
  assert.equal(ask.payload.expects_reply, true)
  assert.equal(ask.generation, 'gen-1')

  const reply = buildAgentMessage({
    fromRunId: 'me', toRunId: 'peer', messageId: 'a1.reply', message: 'ok', role: 'agent',
    replyTo: 'a1',
  })
  assert.equal(reply.payload.reply_to, 'a1')
  assert.equal(reply.payload.expects_reply, undefined)
})

test('parseControlMessage: an ask carries its edge facts, a tell carries none', () => {
  const ask = parseControlMessage({
    type: 'agent_message', from_run_id: 'peer', sender: 'Probe',
    payload: { id: 'a1', from: 'peer', from_run_id: 'peer', to_run_id: 'me', message: 'q', role: 'agent', expects_reply: true },
  })
  assert.deepEqual(ask, {
    fromRunId: 'peer', sender: 'Probe', messageId: 'a1', message: 'q', role: 'agent', expectsReply: true,
  })

  const tell = parseControlMessage({
    type: 'agent_message', from_run_id: 'peer', sender: 'Probe',
    payload: { from: 'peer', from_run_id: 'peer', to_run_id: 'me', message: 'fyi', role: 'agent' },
  })
  assert.equal(tell.messageId, '')
  assert.equal(tell.expectsReply, false)
})

test('parseControlMessage: reply rows and control rows are kept apart', () => {
  // A reply carries the asker's correlation in reply_to; its own id names the
  // edge it completes, and it must not be remembered as a new ask.
  const replyRow = parseControlMessage({
    type: 'agent_message', from_run_id: 'peer', sender: 'Probe',
    payload: { id: 'a1.reply', from_run_id: 'peer', message: 'ok', expects_reply: true },
  })
  assert.equal(replyRow.messageId, 'a1.reply')
  // Control rows are not peer messages at all.
  assert.equal(parseControlMessage({ type: 'continue', payload: { message: 'x' } }), null)
  // A string payload parses; a broken one does not.
  const stringPayload = parseControlMessage({
    type: 'agent_message', sender: 'P',
    payload: JSON.stringify({ id: 'a2', from_run_id: 'peer', message: 'hi' }),
  })
  assert.equal(stringPayload.message, 'hi')
  assert.equal(parseControlMessage({
    type: 'agent_message', sender: 'P', payload: '{oops',
  }), null)
})

test('frameInboundAsk: the sender is broker-stamped, the reply target is the run id', () => {
  const ask = { fromRunId: 'rt_7', sender: 'Probe Peer', messageId: 'a1', message: 'why?', role: '' }
  const text = frameInboundAsk(ask)
  assert.match(text, /peer `Probe Peer`\*\*/)
  assert.match(text, /broker run `rt_7`/)
  assert.match(text, /message id `a1`/)
  assert.match(text, /SendMessage to `avenor:rt_7`/)
  // An unnamed peer falls back to its run id, and tells name no edge.
  const bare = frameInboundAsk({ fromRunId: 'rt_8', sender: '', messageId: '', message: 'm', role: '' })
  assert.match(bare, /peer `rt_8`\*\*/)
  assert.doesNotMatch(bare, /message id/)
})

test('extractReplyMessage: reply text, timeout and cancellation', () => {
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: { message: 'pong' } }), 'pong')
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: JSON.stringify({ message: 'pong2' }) }), 'pong2')
  assert.equal(extractReplyMessage({ timeout: true }), '(timeout: no reply received)')
  assert.equal(extractReplyMessage({ cancelled: true }), '(cancelled)')
  // No message field: the whole row, stringified, so nothing is invented.
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: { other: 1 } }), '{"outcome":"replied","payload":{"other":1}}')
})
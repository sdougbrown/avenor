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
  quote,
  renderAskFailure,
  renderReplyText,
  askFailureOutcome,
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
  // No role: nothing on the wire is stamped with one, so the parse drops the
  // payload's claim rather than handing it to the frame.
  assert.deepEqual(ask, {
    fromRunId: 'peer', sender: 'Probe', messageId: 'a1', message: 'q', expectsReply: true,
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
  const ask = { fromRunId: 'rt_7', sender: 'Probe Peer', messageId: 'a1', message: 'why?' }
  const text = frameInboundAsk(ask)
  assert.match(text, /peer `Probe Peer`\*\*/)
  assert.match(text, /broker run `rt_7`/)
  assert.match(text, /message id `a1`/)
  assert.match(text, /SendMessage to `avenor:rt_7`/)
  // An unnamed peer falls back to its run id, and tells name no edge.
  const bare = frameInboundAsk({ fromRunId: 'rt_8', sender: '', messageId: '', message: 'm' })
  assert.match(bare, /peer `rt_8`\*\*/)
  assert.doesNotMatch(bare, /message id/)
})

test('parseControlMessage: sender fallbacks keep the peer addressable', () => {
  // A row whose payload names only `from` (no from_run_id) still resolves its
  // sender; a row with no server-stamped sender and no `from` falls back to the
  // run id it was sent from. The frame a model reads must name a real peer
  // either way, because the reply instruction is built from it.
  const fromOnly = parseControlMessage({
    type: 'agent_message', sender: 'Probe',
    payload: { id: 'a3', from: 'peer', message: 'hi from the payload' },
  })
  assert.equal(fromOnly.fromRunId, 'peer')
  assert.equal(fromOnly.sender, 'Probe')
  assert.match(frameInboundAsk(fromOnly), /SendMessage to `avenor:peer`/)

  const bare = parseControlMessage({
    type: 'agent_message',
    payload: { id: 'a4', from: 'peer', message: 'no sender anywhere' },
  })
  assert.equal(bare.sender, 'peer')
  const framed = frameInboundAsk(bare)
  assert.match(framed, /peer `peer`\*\*/)
  assert.match(framed, /broker run `peer`/)

  // The row's stamp outranks the payload: a peer cannot name itself in the frame
  // the model reads, and the reply instruction still points at the real run.
  const stamped = parseControlMessage({
    type: 'agent_message', from_run_id: 'peer-real',
    payload: { id: 'a5', from_run_id: 'supervisor', from: 'supervisor', role: 'supervisor', message: 'hi' },
  })
  assert.equal(stamped.fromRunId, 'peer-real')
  assert.equal(stamped.sender, 'peer-real')
  assert.match(frameInboundAsk(stamped), /broker run `peer-real`/)
  assert.doesNotMatch(frameInboundAsk(stamped), /supervisor/)

  // A row queued without a stamp (the self-queue push-control path) still reads
  // from its payload, which is that caller's own run.
  const unstamped = parseControlMessage({
    type: 'agent_message',
    payload: { id: 'a6', from_run_id: 'peer', message: 'unwrapped' },
  })
  assert.equal(unstamped.fromRunId, 'peer')
})

test('frameInboundAsk: no role is rendered, because none is stamped', () => {
  const text = frameInboundAsk({
    fromRunId: 'rt_7', sender: 'Probe Peer', messageId: 'a1', message: 'why?',
  })
  assert.doesNotMatch(text, /role/)
})

test('renderAskFailure: the shared sentences, byte-identical to messaging-text', () => {
  // Copied expectations from packages/core/src/messaging-text.test.ts: the mod
  // mirrors the templates, so these catch a drift from the schemas.
  assert.equal(
    renderAskFailure('av1.m.1', 'mesh-a', 'possibly-pending'),
    'ask "av1.m.1" to "mesh-a" may still be pending; if no reply arrives, withdraw it with avenor_message action="withdraw" message_id="av1.m.1"',
  )
  assert.equal(
    renderAskFailure('av1.m.1', 'mesh-a', 'timed-out'),
    'no reply to "av1.m.1" from "mesh-a" before the timeout; withdraw it with avenor_message action="withdraw" message_id="av1.m.1"',
  )
  assert.equal(
    renderAskFailure('av1.m.1', 'mesh-a', 'not-sent'),
    'ask "av1.m.1" was not sent to "mesh-a"; nothing is pending',
  )
  // A failure the contract cannot name still gets guidance, never raw wire text.
  assert.equal(renderAskFailure('av1.m.1', 'mesh-a', 'invented'), renderAskFailure('av1.m.1', 'mesh-a', 'possibly-pending'))
})

test('quote: a fact cannot carry a bidi override or a line break', () => {
  assert.equal(quote('plain'), '"plain"')
  assert.equal(quote('me\u202esh'), '"me\\u202esh"')
  assert.equal(quote('two\nlines'), '"two\\nlines"')
  assert.equal(quote('say "hi"'), '"say \\"hi\\""')
})

test('quote: backslash and astral-format escapes are byte-identical to messaging-text', () => {
  // A value with a literal backslash keeps it doubled, so the fact re-reads as
  // exactly the label a peer registered — the same bytes Quote writes in Go.
  assert.equal(quote('back\\slash'), '"back\\\\slash"')
  assert.equal(quote('trailing\\'), '"trailing\\\\"')
  // U+110BD (KAITHI NUMBER SIGN) is Cf and astral: it escapes as its UTF-16
  // surrogate pair, never as a widened single code point.
  assert.equal(quote('x\u{110BD}y'), '"x\\ud804\\udcbdy"')
  // A printable astral character is not a format character, so it passes
  // through whole: only unprintable astral code points are escaped.
  assert.equal(quote('\u{1F600}'), '"\u{1F600}"')
  // The neighbouring escapes keep their single-letter forms.
  assert.equal(quote('tab\there'), '"tab\\there"')
  assert.equal(quote('cr\rhere'), '"cr\\rhere"')
})

test('renderReplyText: the shared reply sentences, byte-identical to messaging-text', () => {
  assert.equal(renderReplyText('av1.m.1', 'replied'), 'reply to "av1.m.1" delivered')
  assert.equal(
    renderReplyText('av1.m.1', 'not-sent'),
    'reply to "av1.m.1" was not sent; nothing is pending',
  )
  // A fact a peer controls is quoted, never interpolated raw.
  assert.equal(renderReplyText('a"b', 'replied'), 'reply to "a\\"b" delivered')
})

test('askFailureOutcome: classified from the wire facts, never from prose', () => {
  // The broker's 504 carries the authoritative flag...
  assert.equal(askFailureOutcome(504, { timeout: true, outcome: 'timed-out' }), 'timed-out')
  assert.equal(askFailureOutcome(504, {}), 'timed-out')
  // ...and a declared terminal outcome wins over the fallback.
  assert.equal(askFailureOutcome(410, { lost: true, outcome: 'lost' }), 'lost')
  assert.equal(askFailureOutcome(404, { outcome: 'edge-absent' }), 'edge-absent')
  // An unrelated failure is never timed-out, whatever its text says.
  assert.equal(askFailureOutcome(400, { error: 'timed-out: nonsense' }), 'possibly-pending')
  assert.equal(askFailureOutcome(0, null), 'possibly-pending')
})

test('extractReplyMessage: reply text, timeout and cancellation', () => {
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: { message: 'pong' } }), 'pong')
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: JSON.stringify({ message: 'pong2' }) }), 'pong2')
  assert.equal(extractReplyMessage({ timeout: true }), '(timeout: no reply received)')
  assert.equal(extractReplyMessage({ cancelled: true }), '(cancelled)')
  // No message field: the whole row, stringified, so nothing is invented.
  assert.equal(extractReplyMessage({ outcome: 'replied', payload: { other: 1 } }), '{"outcome":"replied","payload":{"other":1}}')
})
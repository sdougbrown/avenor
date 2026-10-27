// Offline preflight for the avenor peer-layer hooks (issue #391 PR 1).
// Loads the real register.js with a stub engine-call object whose http noun
// speaks to the REAL broker this script started, then drives:
//   1. session.start -> gate, /register, /session_name
//   2. a probe peer sends a bare tell -> idle delivery via prompt.submit
//   3. the probe sends an ask -> pendingReplies correlation
//   4. the session.send hook replies -> probe's /wait_reply unblocks
//   5. busy path: turn.start then ask -> session.append row
//   6. a subagent's turn.start leaves a concurrent ask on prompt.submit
//   7. the mcp__avenor__message hook -> /send expects_reply + /wait_reply,
//      the reply, and the unanswered (timed-out) ask
//   8. a refused prompt.submit falls back to session.append, and the rest of
//      the drained batch still lands
//   9. the ask hook's input refusals never reach the peer
//  10. the version gate on every shape that must stay off, and on the shape
//      that turns the layer on
//  11. the version gate on an old release -> disabled pass-through
//
// Sub-steps pin the edges the main run does not reach: 2b a send after a bare
// tell rides no edge, 4b the reply action answers the ask the hook polled,
// 5b a subagent turn is not the main loop, 6b a timed-out ask is classified
// from the wire facts, 9b a failed /poll-control pauses the loop without
// killing it, 11a a broker-less session stays off with one reason line, 11b a
// partial listing refuses every ref, 11c one ask reads one listing, 11d a
// stale self-minted name is reminted then registered, 11e a failed wait
// retracts its ask.
//
// Every wait is a deadline-bounded poll, not a fixed sleep.
//
// Run: node marketplace/plugins/avenor/test/preflight.mjs http://127.0.0.1:<port>
// (against a quiet `avenor stable` started from this branch)

import { fileURLToPath } from 'node:url'

const PLUGIN = fileURLToPath(new URL('..', import.meta.url))
const BROKER = process.argv[2]

const events = []
const hooks = {}
function record(kind, line) {
  const row = { t: Date.now(), kind, line }
  console.log(JSON.stringify(row))
  events.push(row)
}
function stubOn(event, matcher, hook) {
  const isMatcher = typeof matcher === 'function'
  const hookFn = isMatcher ? matcher : hook
  const hookMatcher = isMatcher ? null : matcher
  hooks[event + (hookMatcher ? JSON.stringify(hookMatcher) : '')] = { event, matcher: hookMatcher, hook: hookFn }
}
function callHook(name) {
  return hooks[name] ?? throwUnknown(name)
}
function throwUnknown(name) {
  throw new Error('no hook: ' + name)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/**
 * Poll until a probe answers with something truthy, or give up loudly. Every
 * wait in this script goes through here: a fixed sleep proves nothing about
 * how long the broker or the loop needs, and a short one flakes.
 */
async function until(what, present, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const found = await present()
    if (found) return found
    if (Date.now() > deadline) {
      console.log('FAIL: timed out waiting for ' + what)
      process.exit(1)
    }
    await sleep(100)
  }
}

/**
 * A probe wait runs alongside the drive, so its rejection must be attributed to
 * the step that started it: an unhandled rejection ends the process with no
 * FAIL line and no step name.
 */
function waitRejected(what, err) {
  console.log('FAIL: the probe wait for ' + what + ' rejected: ' + String(err))
  process.exit(1)
}

// --- the stub call object: http goes over the real broker ---
const env = {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: 'claude-parent',
  AVENOR_SESSION_NAME: 'claude-parent',
}
// Step 9 arms this so one prompt.submit is refused, proving the
// session.append fallback and that the rest of the batch still lands.
let failNextSubmit = false
// Step 10 arms this so one /poll-control request throws: the loop has to pause
// on the failure and keep polling.
let failNextPoll = false
// Every pause the inbound loop takes (the only caller of d.clock.sleep).
let pauses = 0
const d = {
  env: { get: async (n) => env[n] },
  session: {
    version: async () => ({ version: '2.1.311-prod', base: '2.1.311' }),
  },
  clock: {
    now: async () => Date.now(),
    sleep: async (ms) => {
      pauses += 1
      await sleep(ms)
    },
  },
  ui: { log: async (text) => record('ui.log', text) },
  prompt: {
    submit: async (args) => {
      if (failNextSubmit) {
        failNextSubmit = false
        throw new Error('the host refused this prompt.submit')
      }
      record('prompt.submit', args.text)
    },
  },
  // session is overloaded: session.version + session.append
}
d.session.append = async (args) => {
  const text = args.message.content.map((b) => (b && b.text) || '').join('')
  record('session.append', text)
}
// No tool noun: the layer deliberately registers no tool (see register.js).
d.http = {
  fetch: async (url, init = {}) => {
    if (failNextPoll && url.endsWith('/poll-control')) {
      failNextPoll = false
      throw new Error('the poll connection was pulled')
    }
    const res = await fetch(url, init)
    const text = await res.text()
    return { status: res.status, ok: res.ok, headers: {}, text }
  },
}

const mod = await import(PLUGIN + '/hooks/register.js')
await mod.register(stubOn)

// ---------- the probe peer: a plain HTTP client that behaves like a run ----------
let probe = { token: null, runId: 'claude-probe-' + Math.random().toString(16).slice(2, 8) }
async function post(path, body, creds) {
  const res = await fetch(BROKER + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...(creds ? { run_id: probe.runId, token: probe.token } : {}), ...body }),
  })
  const text = await res.text()
  return { status: res.status, ok: res.ok, text }
}
async function registerProbe() {
  const res = await post('/register', { run_id: probe.runId })
  if (!res.ok) throw new Error('register probe failed: ' + res.status + ' ' + res.text)
  const out = JSON.parse(res.text)
  probe.token = out.token
  probe.runId = out.run_id
  await post('/session_name', { name: 'probe-peer' }, true)
  record('probe.registered', JSON.stringify(out))
}
async function probeSendTo(toRunId, fields, creds = true) {
  const payload = {
    id: fields.id,
    from: probe.runId,
    from_run_id: probe.runId,
    to_run_id: toRunId,
    message: fields.message,
    role: 'agent',
  }
  if (fields.replyTo) payload.reply_to = fields.replyTo
  if (fields.expectsReply) payload.expects_reply = true
  return post('/send', {
    from_run_id: probe.runId,
    to_run_id: toRunId,
    type: 'agent_message',
    message_id: fields.id,
    payload,
    ...(fields.generation ? { generation: fields.generation } : {}),
    ...(fields.expectsReply ? { expects_reply: true } : {}),
  }, creds)
}
async function probeWait(messageId, generation, timeoutMs = 30000) {
  const res = await post('/wait_reply', { waiting_for: messageId, generation, timeout_ms: timeoutMs }, true)
  return { status: res.status, ok: res.ok, body: res.text }
}
async function probeSessions() {
  const res = await fetch(BROKER + '/sessions', { headers: { 'X-Avenor-Run-Id': probe.runId, 'X-Avenor-Run-Token': probe.token } })
  return JSON.parse(await res.text())
}
async function probePoll(agentOnly = true) {
  const res = await post('/poll-control', { agent_only: agentOnly }, true)
  if (!res.ok) throw new Error('poll failed: ' + res.status + ' ' + res.text)
  const rows = JSON.parse(res.text)
  return Array.isArray(rows) ? rows : []
}
/**
 * Empty the probe's queue, and report whether anything an ask would arrive as
 * ever showed up: the refusal steps assert it did not.
 */
async function drainProbe() {
  let seen = 0
  for (;;) {
    const rows = await probePoll()
    if (rows.length === 0) return seen
    seen += rows.filter((m) => m && m.type === 'agent_message').length
  }
}

// ---------- drive ----------

console.log('=== 1. session.start ===')
await registerProbe()
await callHook('session.start').hook(d, { cwd: '/tmp', surface: null, isInteractive: true })

const listing = await probeSessions()
const selfRow = listing.sessions.find((s) => s.run_id === 'claude-parent' && s.label === 'claude-parent')
console.log(JSON.stringify({ check: 'self registered + named', row: selfRow }))
if (!selfRow || selfRow.label !== 'claude-parent') {
  console.log('FAIL: self identity not resolvable')
  process.exit(1)
}
if (typeof listing.peers_error === 'string' && listing.peers_error !== '') {
  // The resolution rule keys on this field, so a broker that reports a partial
  // listing refuses every ref below; say so here instead of blaming a send.
  console.log('FAIL: the broker reports a partial peer listing', listing.peers_error)
  process.exit(1)
}

console.log('=== 2. idle delivery: probe sends a bare tell ===')
await probeSendTo('claude-parent', { id: 'tell1', message: 'hello parent, this is a tell' })
await until('the tell to be submitted as a prompt',
  () => events.some((e) => e.kind === 'prompt.submit' && e.line.includes('hello parent')))
const tellRows = events.filter((e) => e.kind === 'prompt.submit')
if (tellRows.length !== 1 || !tellRows[0].line.includes('hello parent')) {
  console.log('FAIL: idle delivery', JSON.stringify(tellRows))
  process.exit(1)
}

console.log('=== 2b. send after a bare tell is a plain tell (no edge correlation) ===')
const plainSend = await callHook('session.send').hook(d, {
  to: `avenor:${probe.runId}`, text: 'hello back, uncorrelated', origin: { kind: 'model' },
})
console.log(JSON.stringify({ check: 'send after tell', plainSend }))
if (!plainSend || plainSend.isDelivered !== true) {
  console.log('FAIL: plain send not delivered')
  process.exit(1)
}
const tellEchoRow = await until('the probe to receive the plain send', async () => {
  const rows = await probePoll()
  return rows.find((m) => m.type === 'agent_message') ?? null
})
console.log(JSON.stringify({ check: 'probe received plain send', tellEchoRow }))
const echoBody = typeof tellEchoRow.payload === 'string' ? JSON.parse(tellEchoRow.payload) : tellEchoRow.payload
if (!echoBody || !String(echoBody.message).includes('hello back')) {
  console.log('FAIL: plain send never reached the probe')
  process.exit(1)
}
if (String(echoBody.id).endsWith('.reply') || echoBody.reply_to !== undefined) {
  console.log('FAIL: a tell was correlated as a reply', echoBody)
  process.exit(1)
}

console.log('=== 3. probe asks (expects_reply) -> correlation held ===')
function mintAskID() {
  const deadline = (Date.now() + 600000).toString(36)
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  const rand = [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('')
  return `av1.${deadline}.${rand}`
}
const askA1 = mintAskID('askA1')
const askSend = await probeSendTo('claude-parent', { id: askA1, message: 'what is the status?', expectsReply: true, generation: listing.generation })
console.log(JSON.stringify({ check: 'probe ask admitted', askSend: askSend.status }))
let probeWaitReturned = null
probeWait(askA1, listing.generation, 30000).then((r) => {
  probeWaitReturned = r
  return r
}).catch((err) => waitRejected('the ask correlation', err))
// the probe polls until the loop picks the ask up
await until('the ask to be submitted as a prompt',
  () => events.filter((e) => e.kind === 'prompt.submit').length >= 2)
const askRows = events.filter((e) => e.kind === 'prompt.submit')
if (askRows.length !== 2 || !askRows[1].line.includes('what is the status?')) {
  console.log('FAIL: ask delivery', JSON.stringify(askRows))
  process.exit(1)
}

console.log('=== 4. session.send hook answers the ask with isDelivered and unblocks probe ===')
const sendResult = await callHook('session.send').hook(d, {
  to: `avenor:${probe.runId}`,
  text: 'pong to the probe',
  origin: { kind: 'model' },
})
console.log(JSON.stringify({ check: 'session.send answer', sendResult }))
if (!sendResult || sendResult.isDelivered !== true) {
  console.log('FAIL: send not claimed delivered')
  process.exit(1)
}
await until('the probe wait to return', () => probeWaitReturned)
const replyShape = probeWaitReturned
console.log(JSON.stringify({ check: 'probe wait_reply', replyShape }))
if (!replyShape || !replyShape.ok || !(replyShape.body || '').includes('pong to the probe')) {
  console.log('FAIL: ask did not unblock')
  process.exit(1)
}

console.log('=== 4b. the reply action answers the ask this session polled ===')
// An ask this session receives lives in the hook's own correlation table, not
// in an edge the MCP server can claim, so the hook has to serve `reply` itself:
// passing it through would answer edge-absent for an ask that is still waiting.
await drainProbe()
const askR1 = mintAskID()
await probeSendTo('claude-parent', { id: askR1, message: 'answer me with the reply action', expectsReply: true, generation: listing.generation })
let replyActionWait = null
probeWait(askR1, listing.generation, 30000).then((r) => {
  replyActionWait = r
  return r
}).catch((err) => waitRejected('the reply-action ask', err))
await until('the reply-action ask to submit',
  () => events.some((e) => e.kind === 'prompt.submit' && e.line.includes('answer me with the reply action')))
const replyActionResult = JSON.parse((await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message', action: 'reply', reply_to: askR1, message: 'answered by the reply action',
})).result)
console.log(JSON.stringify({ check: 'reply action result', replyActionResult }))
if (replyActionResult.action !== 'reply' || replyActionResult.outcome !== 'replied' ||
    replyActionResult.message_id !== askR1 ||
    replyActionResult.text !== 'reply to "' + askR1 + '" delivered') {
  console.log('FAIL: the reply action did not serve the polled ask', replyActionResult)
  process.exit(1)
}
const replyActionShape = await until('the reply-action ask to unblock', () => replyActionWait)
console.log(JSON.stringify({ check: 'reply action unblocked the probe', ok: replyActionShape.ok }))
if (!replyActionShape.ok || !(replyActionShape.body || '').includes('answered by the reply action')) {
  console.log('FAIL: the reply action did not unblock the asker', replyActionShape)
  process.exit(1)
}
// An edge the reply consumed, and a reply_to the hook never held, are both the
// MCP server's to answer: the hook passes them through untouched.
const answeredAgain = await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message', action: 'reply', reply_to: askR1, message: 'answered twice',
})
const unknownReply = await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message', action: 'reply', reply_to: 'av1.unknown.0', message: 'nobody asked',
})
console.log(JSON.stringify({ check: 'reply pass-through', answeredAgain, unknownReply }))
if (answeredAgain !== undefined || unknownReply !== undefined) {
  console.log('FAIL: a reply_to the hook does not hold was served instead of passing through')
  process.exit(1)
}

console.log('=== 5. busy path: main turn started, ask appended instead of submitted ===')
await callHook('turn.start').hook(d, { text: 'busy turn', turnId: 't1' })
const askB1 = mintAskID()
await probeSendTo('claude-parent', { id: askB1, message: 'busy ask should append', expectsReply: true, generation: listing.generation })
// the asker waits, as every real asker does; the busy reply consumes the edge
let busyWaitReturned = null
probeWait(askB1, listing.generation, 30000).then((r) => {
  busyWaitReturned = r
  return r
}).catch((err) => waitRejected('the busy-path ask', err))
await until('the busy ask to append instead of submit',
  () => events.some((e) => e.kind === 'session.append' && e.line.includes('busy ask should append')))
const appendRows = events.filter((e) => e.kind === 'session.append')
if (appendRows.length !== 1 || !appendRows[0].line.includes('busy ask should append')) {
  console.log('FAIL: busy append', JSON.stringify(appendRows))
  process.exit(1)
}
// the busy ask correlation must ride the next reply send
const sendDuringBusy = await callHook('session.send').hook(d, {
  to: `avenor:${probe.runId}`, text: 'answering while busy', origin: { kind: 'model' },
})
console.log(JSON.stringify({ check: 'busy reply send', sendDuringBusy }))
if (!sendDuringBusy || sendDuringBusy.isDelivered !== true) {
  console.log('FAIL: the busy reply send was not delivered')
  process.exit(1)
}
const busyReplyShape = await until('the busy ask to unblock', () => busyWaitReturned)
console.log(JSON.stringify({ check: 'busy ask unblocked', busyReplyShape }))
if (!busyReplyShape.ok || !(busyReplyShape.body || '').includes('answering while busy')) {
  console.log('FAIL: the busy ask did not unblock with its reply')
  process.exit(1)
}
await callHook('turn.complete').hook(d, { answer: 'done', turnId: 't1' })

console.log('=== 5b. a subagent turn is not the main loop: its ask still submits ===')
// Only the main conversation's turns count as busy. A subagent's turn.start
// carries an agentId, so an ask arriving underneath it must still submit.
await callHook('turn.start').hook(d, { text: 'subagent off on its own', turnId: 's1', agentId: 'agent-7' })
const askC1 = mintAskID()
await probeSendTo('claude-parent', { id: askC1, message: 'subagent-time ask should submit', expectsReply: true, generation: listing.generation })
let subagentWaitReturned = null
probeWait(askC1, listing.generation, 30000).then((r) => {
  subagentWaitReturned = r
  return r
}).catch((err) => waitRejected('the subagent-time ask', err))
await until('the subagent-time ask to submit',
  () => events.some((e) => e.kind === 'prompt.submit' && e.line.includes('subagent-time ask should submit')))
const rowsAfterSubagent = events.filter((e) => e.kind === 'session.append')
if (rowsAfterSubagent.length !== 1) {
  console.log('FAIL: a subagent turn flipped an idle ask to an append', JSON.stringify(rowsAfterSubagent))
  process.exit(1)
}
const subagentReply = await callHook('session.send').hook(d, {
  to: `avenor:${probe.runId}`, text: 'answering the subagent-time ask', origin: { kind: 'model' },
})
if (!subagentReply || subagentReply.isDelivered !== true) {
  console.log('FAIL: the subagent-time reply was not delivered')
  process.exit(1)
}
await until('the subagent-time ask to unblock', () => subagentWaitReturned)
await callHook('turn.complete').hook(d, { answer: 'done', turnId: 's1', agentId: 'agent-7' })

console.log('=== 6. the avenor message tool ask action, served by the peer layer ===')
let toolResult = null
callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message',
  action: 'ask',
  target: `avenor:${probe.runId}`,
  message: 'probe, what runs?',
}).then((r) => {
  toolResult = r
  console.log(JSON.stringify({ toolask: 'result', r }))
}, (e) => console.log(JSON.stringify({ toolask: 'error', e: String(e) })))
// the probe polls until it drains the tool's ask
const askedRow = await until('the probe to receive the ask', async () => {
  const rows = await probePoll()
  return rows.find((m) => m.type === 'agent_message') ?? null
})
console.log(JSON.stringify({ check: 'probe received ask', askedRow }))
const raw = askedRow.payload
const askBody = typeof raw === 'string' ? JSON.parse(raw) : raw
const askId = askBody === null || typeof askBody !== 'object' ? '' : askBody.id
if (!askBody || !askId || askBody.message !== 'probe, what runs?') {
  console.log('FAIL: probe never received the ask')
  process.exit(1)
}
if (!askBody.expects_reply) {
  console.log('FAIL: ask did not carry expects_reply')
  process.exit(1)
}
await probeSendTo('claude-parent', { id: askId + '.reply', message: 'the probe answer', replyTo: askId })
const answered = await until('the ask tool to resolve with its reply', () => toolResult)
console.log(JSON.stringify({ check: 'ask tool result', answered }))
if (!JSON.stringify(answered).includes('the probe answer')) {
  console.log('FAIL: ask tool did not get its reply')
  process.exit(1)
}
const answeredShape = JSON.parse(answered.result)
if (answeredShape.outcome !== 'replied' || answeredShape.reply !== 'the probe answer' || answeredShape.message_id !== askId) {
  console.log('FAIL: the replied ask result is not the router shape', answeredShape)
  process.exit(1)
}

console.log('=== 6b. an unanswered ask is timed-out, with the contract sentence ===')
// The probe never answers this one, so the bounded wait expires and the broker
// answers 504. The hook classifies the timeout from the wire facts and renders
// the shared ask sentence, never the raw HTTP text.
const timedOutAsk = JSON.parse((await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message',
  action: 'ask',
  target: `avenor:${probe.runId}`,
  message: 'this one goes unanswered',
  timeout_ms: 500,
})).result)
console.log(JSON.stringify({ check: 'ask tool timeout shape', timedOutAsk }))
if (timedOutAsk.outcome !== 'timed-out') {
  console.log('FAIL: an unanswered ask is not timed-out', timedOutAsk)
  process.exit(1)
}
if (typeof timedOutAsk.message_id !== 'string' || timedOutAsk.message_id === '') {
  console.log('FAIL: the timed-out ask carries no message id to withdraw with', timedOutAsk)
  process.exit(1)
}
if (!timedOutAsk.text.includes('withdraw it with avenor_message action="withdraw" message_id="' + timedOutAsk.message_id + '"')) {
  console.log('FAIL: the timed-out ask does not render the shared withdraw guidance', timedOutAsk)
  process.exit(1)
}
if (timedOutAsk.reply !== undefined) {
  console.log('FAIL: a timed-out ask carried a reply', timedOutAsk)
  process.exit(1)
}

console.log('=== 7. unknown avenor ref (before any gate) ===')
const miss = await callHook('session.send').hook(d, { to: 'avenor:nope', text: 'z', origin: { kind: 'model' } })
console.log(JSON.stringify({ check: 'unknown ref', miss }))
if (!miss || miss.isDelivered !== false || !miss.reason.includes('no avenor run matches')) {
  console.log('FAIL: unknown ref behavior')
  process.exit(1)
}

console.log('=== 8. pass-through: non-avenor address ===')
const passthrough = await callHook('session.send').hook(d, { to: 'some teammate', text: 'kept', origin: { kind: 'model' } })
console.log(JSON.stringify({ check: 'pass-through', passthrough }))
if (passthrough !== undefined) {
  console.log('FAIL: non-avenor send was intercepted')
  process.exit(1)
}

console.log('=== 9. a refused prompt.submit falls back to session.append; the batch still lands ===')
// The host can refuse a submitted prompt. That row still has to reach the
// conversation, and the ask behind it in the same drained batch must still be
// delivered — a failed submit must not stop the poll loop.
await drainProbe()
failNextSubmit = true
const askD1 = mintAskID()
const askD2 = mintAskID()
const batch1 = await probeSendTo('claude-parent', { id: askD1, message: 'the refused submit row', expectsReply: true, generation: listing.generation })
const batch2 = await probeSendTo('claude-parent', { id: askD2, message: 'the row behind it', expectsReply: true, generation: listing.generation })
console.log(JSON.stringify({ check: 'batch queued', batch1: batch1.status, batch2: batch2.status }))
const fellBack = await until('the refused submit to land as a row',
  () => events.find((e) => e.kind === 'session.append' && e.line.includes('the refused submit row')))
console.log(JSON.stringify({ check: 'refused submit fell back to append', fellBack: fellBack.line.slice(0, 80) }))
const behind = await until('the row behind it to be delivered',
  () => events.find((e) => e.kind === 'prompt.submit' && e.line.includes('the row behind it')))
console.log(JSON.stringify({ check: 'the batch kept draining', delivered: behind.line.slice(0, 80) }))
if (events.filter((e) => e.kind === 'prompt.submit' && e.line.includes('the refused submit row')).length !== 0) {
  console.log('FAIL: the refused submit was recorded as a submit')
  process.exit(1)
}

console.log('=== 9b. a failed /poll-control pauses the inbound loop, which survives it ===')
// The loop's contract is that every failure pauses it and no iteration kills
// it. One thrown poll must cost exactly one pause, and the ask queued meanwhile
// must still arrive on the next poll with its edge intact.
await drainProbe()
const pausesBefore = pauses
failNextPoll = true
const askP1 = mintAskID()
await probeSendTo('claude-parent', { id: askP1, message: 'delivered after a failed poll', expectsReply: true, generation: listing.generation })
let pollWaitReturned = null
probeWait(askP1, listing.generation, 30000).then((r) => {
  pollWaitReturned = r
  return r
}).catch((err) => waitRejected('the post-failure ask', err))
await until('the inbound loop to pause after the failed poll', () => pauses > pausesBefore)
const afterFailure = await until('the ask behind the failed poll to be delivered',
  () => events.find((e) => e.kind === 'prompt.submit' && e.line.includes('delivered after a failed poll')))
console.log(JSON.stringify({
  check: 'poll failure paused then resumed',
  pauses: pauses - pausesBefore,
  delivered: afterFailure.line.slice(0, 60),
}))
const afterFailureReply = await callHook('session.send').hook(d, {
  to: `avenor:${probe.runId}`, text: 'answering after the failed poll', origin: { kind: 'model' },
})
if (!afterFailureReply || afterFailureReply.isDelivered !== true) {
  console.log('FAIL: the reply after a failed poll was not delivered')
  process.exit(1)
}
const afterFailureWait = await until('the post-failure ask to unblock', () => pollWaitReturned)
if (!afterFailureWait.ok || !(afterFailureWait.body || '').includes('answering after the failed poll')) {
  console.log('FAIL: the post-failure ask did not unblock with its reply', afterFailureWait)
  process.exit(1)
}

console.log('=== 10. the ask hook refuses bad input without touching the broker ===')
await drainProbe()
const beforeRefusals = await probePoll()
if (beforeRefusals.length !== 0) {
  console.log('FAIL: the probe queue was not drained before the refusal step', JSON.stringify(beforeRefusals))
  process.exit(1)
}
const emptyMessage = JSON.parse((await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message', action: 'ask', target: `avenor:${probe.runId}`, message: '',
})).result)
const foreignTarget = JSON.parse((await callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message', action: 'ask', target: 'some teammate', message: 'hi',
})).result)
console.log(JSON.stringify({ check: 'ask refusals', emptyMessage, foreignTarget }))
if (emptyMessage.outcome !== 'not-sent' || emptyMessage.message_id !== undefined ||
    !emptyMessage.text.includes('non-empty string')) {
  console.log('FAIL: an empty ask message was not refused', emptyMessage)
  process.exit(1)
}
if (foreignTarget.outcome !== 'not-sent' || foreignTarget.message_id !== undefined ||
    !foreignTarget.text.includes('must be an avenor:')) {
  console.log('FAIL: a non-avenor ask target was not refused', foreignTarget)
  process.exit(1)
}
// Nothing was minted or sent, so no row can have reached the probe.
const afterRefusals = await probePoll()
const strayRows = afterRefusals.filter((m) => m && m.type === 'agent_message')
console.log(JSON.stringify({ check: 'no row reached the probe', strayRows: strayRows.length }))
if (strayRows.length !== 0) {
  console.log('FAIL: a refused ask still reached the peer', JSON.stringify(strayRows))
  process.exit(1)
}

console.log('=== 11. the version gate on every shape ===')
// Each shape runs in its own module copy (a cache-busting import) so the armed
// layer above keeps its state. A shape that does not parse as three numeric
// parts is below the floor: the layer logs one line and stays off, and with no
// broker configured the log proves it never reached registration.
async function layerCopy(versionAnswer, envValues, httpFetch) {
  gateCopies += 1
  const copyHooks = {}
  const copyEnv = { ...envValues }
  const copyD = {
    env: { get: async (n) => copyEnv[n] },
    session: { version: async () => versionAnswer, append: d.session.append },
    clock: d.clock,
    ui: d.ui,
    prompt: d.prompt,
    http: httpFetch === undefined ? d.http : { fetch: httpFetch },
  }
  const mod = await import(PLUGIN + '/hooks/register.js?gate=' + gateCopies)
  await mod.register((event, matcher, hook) => {
    const isMatcher = typeof matcher === 'function'
    const hookFn = isMatcher ? matcher : hook
    const hookMatcher = isMatcher ? null : matcher
    copyHooks[event + (hookMatcher ? JSON.stringify(hookMatcher) : '')] = hookFn
  })
  await copyHooks['session.start'](copyD, { cwd: '/tmp' })
  return { d: copyD, hooks: copyHooks }
}
let gateCopies = 0

/** A copy whose /sessions answer is fixed, and whose every other call is real. */
function listingStub(body, onRead) {
  return async (url, init = {}) => {
    if (url.endsWith('/sessions')) {
      onRead()
      return { status: 200, ok: true, headers: {}, text: JSON.stringify(body) }
    }
    const res = await fetch(url, init)
    const text = await res.text()
    return { status: res.status, ok: res.ok, headers: {}, text }
  }
}
function freshRunId(prefix) {
  return prefix + '-' + Math.random().toString(16).slice(2, 8)
}
function askHookCall(copy, fields) {
  return copy.hooks['tool.call{"tool":"mcp__avenor__message"}'](copy.d, {
    tool: 'mcp__avenor__message', action: 'ask', ...fields,
  })
}

const offShapes = [
  { what: 'no base, a suffixed version', answer: { version: '2.1.311-prod' }, logged: '2.1.311-prod' },
  { what: 'a four-part version', answer: { base: '2.1.311.1' }, logged: '2.1.311.1' },
  { what: 'a non-numeric patch', answer: { base: '2.1.x' }, logged: '2.1.x' },
]
for (const shape of offShapes) {
  const copy = await layerCopy(shape.answer, {})
  const offLine = events.find((e) => e.kind === 'ui.log' && e.line.includes(shape.logged) && e.line.includes('below the tested floor'))
  const reachedRegistration = events.find((e) => e.kind === 'ui.log' && e.line.includes('no avenor broker is configured'))
  console.log(JSON.stringify({ check: 'version shape stays off', what: shape.what, logged: Boolean(offLine) }))
  if (!offLine) {
    console.log('FAIL: ' + shape.what + ' did not log the version refusal')
    process.exit(1)
  }
  if (reachedRegistration) {
    console.log('FAIL: ' + shape.what + ' reached registration instead of the gate')
    process.exit(1)
  }
  const offSend = await copy.hooks['session.send'](copy.d, { to: `avenor:${probe.runId}`, text: 'x', origin: { kind: 'model' } })
  if (offSend !== undefined) {
    console.log('FAIL: ' + shape.what + ' left the layer armed')
    process.exit(1)
  }
}
console.log('=== 11a. a passing version with no broker configured keeps the layer off ===')
// The graceful disable: the version gate passes, the broker is missing, so the
// layer explains itself once and stays off — no registration attempt, no
// subscription, and every send passes through.
const armedBefore = events.filter((e) => e.line && e.line.includes('peer layer on')).length
const noBroker = await layerCopy({ version: '2.1.311-prod', base: '2.1.311' }, {})
const noBrokerLines = events.filter((e) => e.kind === 'ui.log' && e.line.includes('no avenor broker is configured'))
const armedAfter = events.filter((e) => e.line && e.line.includes('peer layer on')).length
console.log(JSON.stringify({
  check: 'no broker keeps the layer off',
  reasonLines: noBrokerLines.length,
  reason: noBrokerLines.length === 1 ? noBrokerLines[0].line : null,
  armed: armedAfter - armedBefore,
}))
if (noBrokerLines.length !== 1 || !noBrokerLines[0].line.includes('AVENOR_BROKER_URL')) {
  console.log('FAIL: a broker-less session did not log its reason exactly once')
  process.exit(1)
}
if (armedAfter !== armedBefore) {
  console.log('FAIL: a broker-less session armed the layer')
  process.exit(1)
}
const noBrokerSend = await noBroker.hooks['session.send'](noBroker.d, {
  to: `avenor:${probe.runId}`, text: 'x', origin: { kind: 'model' },
})
console.log(JSON.stringify({ check: 'no-broker send passes through', noBrokerSend }))
if (noBrokerSend !== undefined) {
  console.log('FAIL: the broker-less layer intercepted an avenor:* send')
  process.exit(1)
}

// The shape the floor accepts turns the layer on for real: a fresh run id, so
// the enabled copy registers and resolves off the live broker.
const onCopy = await layerCopy({ version: '2.1.311-prod', base: '2.1.311' }, {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: 'claude-gate-' + Math.random().toString(16).slice(2, 8),
})
const onLine = events.find((e) => e.kind === 'ui.log' && e.line.includes('peer layer on (run claude-gate-'))
console.log(JSON.stringify({ check: 'a valid shape turns the layer on', logged: Boolean(onLine) }))
if (!onLine) {
  console.log('FAIL: a valid version shape did not arm the layer')
  process.exit(1)
}
const onSend = await onCopy.hooks['session.send'](onCopy.d, { to: 'avenor:nobody-here', text: 'x', origin: { kind: 'model' } })
if (!onSend || onSend.isDelivered !== false || !onSend.reason.includes('no avenor run matches')) {
  console.log('FAIL: the armed copy did not resolve through the broker', onSend)
  process.exit(1)
}

console.log('=== 11b. a partial peer listing refuses every ref instead of matching a subset ===')
// The row IS in the listing the broker returned, so a resolver that matched the
// subset would deliver to it; a listing flagged peers_error must refuse instead.
const partial = await layerCopy({ base: '2.1.311' }, {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: freshRunId('claude-partial'),
}, listingStub({
  sessions: [{ run_id: 'visible-run', label: 'visible' }],
  generation: 'gen-partial',
  peers_error: 'the broker could not read every peer',
}, () => {}))
const partialSend = await partial.hooks['session.send'](partial.d, {
  to: 'avenor:visible', text: 'x', origin: { kind: 'model' },
})
const partialAsk = JSON.parse((await askHookCall(partial, { target: 'avenor:visible', message: 'hi' })).result)
console.log(JSON.stringify({ check: 'partial listing refusals', partialSend, partialAsk }))
if (!partialSend || partialSend.isDelivered !== false ||
    !partialSend.reason.includes('the peer listing is incomplete')) {
  console.log('FAIL: a send resolved against a partial listing', partialSend)
  process.exit(1)
}
if (partialAsk.outcome !== 'not-sent' || partialAsk.message_id !== undefined ||
    !partialAsk.text.includes('the peer listing is incomplete')) {
  console.log('FAIL: an ask resolved against a partial listing', partialAsk)
  process.exit(1)
}

console.log('=== 11c. an ask resolves and pins its generation from one listing ===')
const beforeAsk = await probeSessions()
let listingsRead = 0
const oneCopy = await layerCopy({ base: '2.1.311' }, {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: freshRunId('claude-onelisting'),
}, listingStub({
  sessions: beforeAsk.sessions,
  generation: beforeAsk.generation,
  messaging_protocol: beforeAsk.messaging_protocol,
}, () => { listingsRead += 1 }))
const oneAsk = JSON.parse((await askHookCall(oneCopy, {
  target: `avenor:${probe.runId}`, message: 'one listing please', timeout_ms: 300,
})).result)
console.log(JSON.stringify({ check: 'ask listings read', listingsRead, outcome: oneAsk.outcome }))
if (oneAsk.outcome !== 'timed-out') {
  console.log('FAIL: the ask against a known peer did not reach its wait', oneAsk)
  process.exit(1)
}
if (listingsRead !== 1) {
  console.log('FAIL: one ask read the peer listing ' + listingsRead + ' times')
  process.exit(1)
}
await drainProbe()

console.log('=== 11d. a stale self-minted name is reminted once, then registered ===')
// A self-minted `claude-<hex>` can collide with a record the broker still holds
// from an earlier session, which this session cannot authenticate against. One
// refusal on that basis mints a fresh suffix and retries; the session then owns
// the reminted run.
const remintAttempts = []
await layerCopy({ base: '2.1.311' }, { AVENOR_BROKER_URL: BROKER }, async (url, init = {}) => {
  if (url.endsWith('/register')) {
    const body = JSON.parse(init.body === undefined ? '{}' : init.body)
    remintAttempts.push(body.run_id)
    if (remintAttempts.length === 1) {
      return {
        status: 409,
        ok: false,
        headers: {},
        text: JSON.stringify({ error: 'run "' + body.run_id + '" is already registered' }),
      }
    }
  }
  const res = await fetch(url, init)
  const text = await res.text()
  return { status: res.status, ok: res.ok, headers: {}, text }
})
console.log(JSON.stringify({ check: 'self-minted names tried', remintAttempts }))
if (remintAttempts.length !== 2) {
  console.log('FAIL: a stale self-minted name did not retry exactly once', remintAttempts)
  process.exit(1)
}
if (remintAttempts[1] === remintAttempts[0] || !remintAttempts[1].startsWith('claude-')) {
  console.log('FAIL: the retry did not mint a fresh suffix', remintAttempts)
  process.exit(1)
}
const remintLine = events.find((e) => e.line === 'peer layer on (run ' + remintAttempts[1] + ', broker ' + BROKER + ')')
if (!remintLine) {
  console.log('FAIL: the layer did not arm with the reminted run', remintAttempts)
  process.exit(1)
}
const afterRemint = await probeSessions()
const remintRow = afterRemint.sessions.find((s) => s.run_id === remintAttempts[1])
const refusedRow = afterRemint.sessions.find((s) => s.run_id === remintAttempts[0])
console.log(JSON.stringify({ check: 'the reminted run is registered', remintRow, refused: Boolean(refusedRow) }))
if (!remintRow || refusedRow) {
  console.log('FAIL: the session did not end up registered under the reminted name', remintAttempts)
  process.exit(1)
}

console.log('=== 11e. a failed wait retracts its ask, leaving no edge to answer ===')
// A wait that fails for a reason the broker did not classify — a cut connection
// rather than its own 504 — leaves the ask's edge live. The hook retracts it, as
// the pi and core ask paths do, so a reply arriving afterwards finds no edge
// instead of landing where no waiter will read it.
let waitCutOnce = false
const retractRunId = freshRunId('claude-retract')
const retractCopy = await layerCopy({ base: '2.1.311' }, {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: retractRunId,
}, async (url, init = {}) => {
  if (url.endsWith('/wait_reply') && !waitCutOnce) {
    waitCutOnce = true
    throw new Error('the wait connection was cut')
  }
  const res = await fetch(url, init)
  const text = await res.text()
  return { status: res.status, ok: res.ok, headers: {}, text }
})
const cutAsk = JSON.parse((await askHookCall(retractCopy, {
  target: `avenor:${probe.runId}`, message: 'the wait gets cut', timeout_ms: 5000,
})).result)
console.log(JSON.stringify({ check: 'a cut wait reports its failure', cutAsk }))
if (cutAsk.outcome !== 'possibly-pending' || cutAsk.reply !== undefined) {
  console.log('FAIL: a cut wait did not report possibly-pending', cutAsk)
  process.exit(1)
}
await drainProbe()
const retractedReply = await probeSendTo(retractRunId, {
  id: cutAsk.message_id + '.reply',
  message: 'a late answer to a retracted ask',
  replyTo: cutAsk.message_id,
})
const retractedBody = JSON.parse(retractedReply.text)
console.log(JSON.stringify({ check: 'a reply after the retraction finds no edge', retractedBody }))
if (retractedBody.outcome !== 'edge-absent') {
  console.log('FAIL: a failed wait left its ask edge live', retractedBody)
  process.exit(1)
}

console.log('=== 12. version gate on an old release (runs last: the gate is module state) ===')
const oldEnv = { ...env, AVENOR_BROKER_URL: BROKER }
delete oldEnv.AVENOR_RUN_ID
delete oldEnv.AVENOR_SESSION_NAME
const oldD = {
  env: { get: async (n) => oldEnv[n] },
  session: { version: async () => ({ version: '2.1.280-prod', base: '2.1.280' }), append: d.session.append },
  clock: d.clock,
  ui: d.ui,
  prompt: d.prompt,
  http: d.http,
}
await callHook('session.start').hook(oldD, {})
const gateRows = events.filter((e) => e.line && e.line.includes('2.1.280'))
if (gateRows.length === 0) {
  console.log('FAIL: no gate log line')
  process.exit(1)
}
// disabled means disabled: even an avenor:* send passes through untouched
const gatedSend = await callHook('session.send').hook(d, { to: `avenor:${probe.runId}`, text: 'x', origin: { kind: 'model' } })
console.log(JSON.stringify({ check: 'gated session.send passes through', gatedSend }))
if (gatedSend !== undefined) {
  console.log('FAIL: the disabled layer intercepted an avenor:* send')
  process.exit(1)
}

console.log('PREFLIGHT OK')
console.log(JSON.stringify({ eventsRecorded: events.length, moduleCopies: gateCopies }))
// Every armed copy runs an inbound poll loop for the session's life, so the
// event loop would never drain on its own: end the drive deliberately.
process.exit(0)
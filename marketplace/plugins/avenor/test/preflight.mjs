// Offline preflight for the avenor peer-layer hooks (issue #391 PR 1).
// Loads the real register.js with a stub engine-call object whose http noun
// speaks to the REAL broker this script started, then drives:
//   1. session.start -> gate, /register, /session_name, tool.register
//   2. a probe peer sends a bare tell -> idle delivery via prompt.submit
//   3. the probe sends an ask -> pendingReplies correlation
//   4. the session.send hook replies -> probe's /wait_reply unblocks
//   5. busy path: turn.start then ask -> session.append row
//   6. the mcp__avenor__ask tool hook -> /send expects_reply + /wait_reply
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

// --- the stub call object: http goes over the real broker ---
const env = {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: 'claude-parent',
  AVENOR_SESSION_NAME: 'claude-parent',
}
const d = {
  env: { get: async (n) => env[n] },
  session: {
    version: async () => ({ version: '2.1.311-prod', base: '2.1.311' }),
  },
  clock: {
    now: async () => Date.now(),
    sleep: (ms) => new Promise((res) => setTimeout(res, ms)),
  },
  ui: { log: async (text) => record('ui.log', text) },
  prompt: { submit: async (args) => record('prompt.submit', args.text) },
  sessionAppend: undefined,
  // session is overloaded: session.version + session.append
}
d.session.append = async (args) => {
  const text = args.message.content.map((b) => (b && b.text) || '').join('')
  record('session.append', text)
}
const registeredTools = []
d.tool = {
  register: async (spec) => {
    registeredTools.push(spec)
    record('tool.register', spec.name)
    return { tool: 'mcp__avenor__' + spec.name }
  },
}
d.http = {
  fetch: async (url, init = {}) => {
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

// ---------- drive ----------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

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

console.log('=== 2. idle delivery: probe sends a bare tell ===')
await probeSendTo('claude-parent', { id: 'tell1', message: 'hello parent, this is a tell' })
await sleep(3500)
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
await sleep(2500)
const tellEchoRow = (await probePoll()).find((m) => m.type === 'agent_message') ?? null
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
const waitPromise = probeWait(askA1, listing.generation, 30000).then((r) => {
  probeWaitReturned = r
  return r
})
await sleep(4000) // let the poll pick the ask up
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
await Promise.race([waitPromise, sleep(30000)])
const replyShape = probeWaitReturned
console.log(JSON.stringify({ check: 'probe wait_reply', replyShape }))
if (!replyShape || !replyShape.ok || !(replyShape.body || '').includes('pong to the probe')) {
  console.log('FAIL: ask did not unblock')
  process.exit(1)
}

console.log('=== 5. busy path: main turn started, ask appended instead of submitted ===')
await callHook('turn.start').hook(d, { text: 'busy turn', turnId: 't1' })
const askB1 = mintAskID()
await probeSendTo('claude-parent', { id: askB1, message: 'busy ask should append', expectsReply: true, generation: listing.generation })
// the asker waits, as every real asker does; the busy reply consumes the edge
let busyWaitReturned = null
const busyWaitPromise = probeWait(askB1, listing.generation, 30000).then((r) => {
  busyWaitReturned = r
  return r
})
await sleep(4000)
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
await Promise.race([busyWaitPromise, sleep(30000)])
console.log(JSON.stringify({ check: 'busy ask unblocked', busyWaitReturned }))
await callHook('turn.complete').hook(d, { answer: 'done', turnId: 't1' })

console.log('=== 6. the avenor message tool ask action, served by the peer layer ===')
const askToolPromise = callHook('tool.call{"tool":"mcp__avenor__message"}').hook(d, {
  tool: 'mcp__avenor__message',
  action: 'ask',
  target: `avenor:${probe.runId}`,
  message: 'probe, what runs?',
})
askToolPromise.then((r) => console.log(JSON.stringify({ toolask: 'result', r })),
  (e) => console.log(JSON.stringify({ toolask: 'error', e: String(e) })))
await sleep(1500)
// the probe polls until it drains the tool's ask
let askedRow = null
for (let i = 0; i < 10 && !askedRow; i += 1) {
  const rows = await probePoll()
  askedRow = rows.find((m) => m.type === 'agent_message') ?? null
  if (!askedRow) await sleep(1000)
}
console.log(JSON.stringify({ check: 'probe received ask', askedRow }))
let askId = null
let askBody = null
const raw = askedRow.payload
askBody = typeof raw === 'string' ? JSON.parse(raw) : raw
askId = askBody.id
if (!askBody || !askId || askBody.message !== 'probe, what runs?') {
  console.log('FAIL: probe never received the ask')
  process.exit(1)
}
if (!askBody.expects_reply) {
  console.log('FAIL: ask did not carry expects_reply')
  process.exit(1)
}
await probeSendTo('claude-parent', { id: askId + '.reply', message: 'the probe answer', replyTo: askId })
const toolResult = await Promise.race([askToolPromise, sleep(30000)])
console.log(JSON.stringify({ check: 'ask tool result', toolResult }))
if (!toolResult || !JSON.stringify(toolResult).includes('the probe answer')) {
  console.log('FAIL: ask tool did not get its reply')
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

console.log('=== 9. version gate on an old release (runs last: the gate is module state) ===')
const oldEnv = { ...env, AVENOR_BROKER_URL: BROKER }
delete oldEnv.AVENOR_RUN_ID
delete oldEnv.AVENOR_SESSION_NAME
const oldD = {
  env: { get: async (n) => oldEnv[n] },
  session: { version: async () => ({ version: '2.1.280-prod', base: '2.1.280' }), append: d.session.append },
  clock: d.clock,
  ui: d.ui,
  prompt: d.prompt,
  tool: d.tool,
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
console.log(JSON.stringify({ eventsRecorded: events.length, registeredTools: registeredTools.map((t) => t.name) }))
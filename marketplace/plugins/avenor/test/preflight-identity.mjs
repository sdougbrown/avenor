// Register-identity preflight for the avenor peer layer (issue #391, PR 2).
// Loads the real hooks/register.js against a stub engine-call object whose http
// noun speaks to a REAL broker, and drives session.start once per identity the
// layer can be handed:
//
//   A. provisioned — broker URL, run id and this launch's one-shot attach
//      credential name a run the launcher already holds, and AVENOR_LABEL
//      carries the launch label (every avenor launch exports its identity that
//      way). The layer trades the credential at /attach for the run's own and
//      writes nothing else about the run: no /register, no /session_name. The
//      name write is asserted absent by path — a harness name outranks the
//      record's label in the broker, so posting the launch label would shadow
//      the name peers address the run by, including through a later authorized
//      rename.
//   B. half-provisioned — a run id without a credential. The layer
//      self-registers through the broker's open /register route and posts the
//      name it was handed, here as AVENOR_LABEL alone.
//   C. bare — no run id at all. The layer mints its own run and posts its name,
//      handed as the older AVENOR_SESSION_NAME spelling a shell may still set.
//   D. a second session holding A's environment. The attach credential was
//      spent by A, so the layer disables itself with the broker's reason and
//      polls nothing: one run keeps one inbound consumer.
//
// Run against a real `avenor stable` broker:
//   node marketplace/plugins/avenor/test/preflight-identity.mjs http://127.0.0.1:<port>
//
// Prints one JSON row per check and exits non-zero on the first failed group.

import { fileURLToPath } from 'node:url'

const PLUGIN = fileURLToPath(new URL('..', import.meta.url))
const BROKER = process.argv[2]
if (!BROKER) {
  console.error('usage: node preflight-identity.mjs <broker-url>')
  process.exit(2)
}

const events = []
function record(kind, line) {
  const row = { kind, line }
  console.log(JSON.stringify(row))
  events.push(row)
}

let failures = 0
function check(name, ok, detail) {
  console.log(JSON.stringify({ check: name, ok, detail }))
  if (!ok) failures += 1
}

// The preflight's own traffic (provisioning, listings, renames) goes over plain
// fetch; only the requests the layer under test issues are recorded.
async function post(path, body, creds) {
  const res = await fetch(BROKER + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ...(creds ? { run_id: creds.runId, token: creds.token } : {}), ...body }),
  })
  return { status: res.status, ok: res.ok, text: await res.text() }
}

async function sessions(creds) {
  const res = await fetch(BROKER + '/sessions', {
    headers: { 'X-Avenor-Run-Id': creds.runId, 'X-Avenor-Run-Token': creds.token },
  })
  if (!res.ok) throw new Error('GET /sessions: ' + res.status)
  return JSON.parse(await res.text())
}

async function labelOf(creds, runId) {
  const listing = await sessions(creds)
  const row = listing.sessions.find((s) => s.run_id === runId)
  return row ? row.label : null
}

// The broker's open register route writes the record a supervisor's
// EnsureRun leaves behind when it is handed the token; the label follows
// through /rename, which is the route an authorized name change takes. The
// answer's `attach` is the one-shot credential a launch hands its child, and
// it is what the provisioned layer case presents.
const STAMP = Date.now().toString(36)
async function provisionRun(runId, token) {
  const res = await post('/register', { run_id: runId, token })
  if (!res.ok) throw new Error('provision ' + runId + ': ' + res.status + ' ' + res.text)
  const answer = JSON.parse(res.text)
  if (typeof answer.attach !== 'string' || answer.attach === '') {
    throw new Error('provision ' + runId + ': the broker minted no attach credential')
  }
  return { runId, token, attach: answer.attach }
}
async function labelRun(creds, name) {
  const res = await post('/rename', { name }, creds)
  if (!res.ok) throw new Error('rename ' + creds.runId + ': ' + res.status + ' ' + res.text)
  return JSON.parse(res.text)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

/**
 * Deadline-bounded wait for a condition a broker round trip has to reach. Every
 * wait in this drive is bounded: a hang is a failure, never a slow pass.
 */
async function until(match, budgetMs = 15000) {
  const deadline = Date.now() + budgetMs
  for (;;) {
    if (match()) return true
    if (Date.now() > deadline) return false
    await sleep(100)
  }
}

/** The prompts the layer under test submitted, in arrival order. */
function submitted(caseName) {
  return events.filter((e) => e.kind === caseName + ' prompt.submit').map((e) => e.line)
}

// ---------- the stub engine-call object ----------
async function loadLayer(caseName, env) {
  const hooks = {}
  const requests = []
  function on(event, matcher, hook) {
    hooks[event] = typeof matcher === 'function' ? matcher : hook
  }
  const stub = {
    env: { get: async (name) => env[name] },
    session: {
      version: async () => ({ version: '2.1.311', base: '2.1.311' }),
      append: async (args) => record(caseName + ' session.append', JSON.stringify(args)),
    },
    clock: { now: async () => Date.now(), sleep: (ms) => new Promise((r) => setTimeout(r, ms)) },
    ui: { log: async (text) => record(caseName + ' ui.log', text) },
    prompt: { submit: async (args) => record(caseName + ' prompt.submit', args.text) },
    http: {
      fetch: async (url, init = {}) => {
        const target = String(url)
        requests.push({
          method: init.method ?? 'GET',
          path: target.startsWith(BROKER) ? target.slice(BROKER.length) : target,
        })
        const res = await fetch(url, init)
        return { status: res.status, ok: res.ok, headers: {}, text: await res.text() }
      },
    },
    tool: { register: async () => { throw new Error('no tool registration expected') } },
  }
  // A query string gives each case its own module instance: the layer's state
  // (broker, armed flag, poll loop) is module-scoped.
  const mod = await import(PLUGIN + '/hooks/register.js?identity=' + encodeURIComponent(caseName))
  await mod.register(on)
  return { caseName, d: stub, hooks, requests }
}

function issued(layer, path) {
  return layer.requests.filter((r) => r.method === 'POST' && r.path === path)
}

// ---------- A. provisioned identity ----------
console.log('=== A. a provisioned run joins without writing its identity ===')
const RUN_A = 'pf-prov-' + STAMP
const NAME_A = 'pf-child-a'
const provisionedA = await provisionRun(RUN_A, 'preflight-provisioned-token')
await labelRun(provisionedA, NAME_A)
const beforeA = await sessions(provisionedA)

// The launch hands its child the run id and the one-shot credential minted for
// this launch, never the run's own.
const envA = {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: RUN_A,
  AVENOR_BROKER_TOKEN: provisionedA.attach,
  // The launch label, exactly as a supervisor spawn passes it.
  AVENOR_LABEL: NAME_A,
}
const layerA = await loadLayer('a', envA)
await layerA.hooks['session.start'](layerA.d, { cwd: '/tmp', isInteractive: true })

const afterA = await sessions(provisionedA)
const rowA = afterA.sessions.find((s) => s.run_id === RUN_A)
check('A joined the provisioned run as its launch label', Boolean(rowA) && rowA.label === NAME_A, rowA)
check('A issued exactly one /attach', issued(layerA, '/attach').length === 1, layerA.requests.map((r) => r.method + ' ' + r.path))
check('A issued no /register', issued(layerA, '/register').length === 0, layerA.requests.map((r) => r.method + ' ' + r.path))
check('A issued no /session_name write', issued(layerA, '/session_name').length === 0,
  layerA.requests.map((r) => r.method + ' ' + r.path))
check('A minted no second run', afterA.sessions.length === beforeA.sessions.length,
  { before: beforeA.sessions.length, after: afterA.sessions.length })
const logsA = events.filter((e) => e.kind === 'a ui.log').map((e) => e.line).join('\n')
check('A logs the provisioned path', logsA.includes('peer layer on (provisioned run ' + RUN_A), logsA)

// The provisioned token still drives both directions.
const probeA = await provisionRun('pf-probe-a-' + STAMP, 'preflight-probe-a-token')
const tellId = 'av1.preflight.' + STAMP
const tell = await post('/send', {
  from_run_id: probeA.runId,
  to_run_id: RUN_A,
  type: 'agent_message',
  message_id: tellId,
  payload: {
    id: tellId,
    from: probeA.runId,
    from_run_id: probeA.runId,
    to_run_id: RUN_A,
    message: 'preflight-tell-arrives',
    role: 'agent',
  },
}, probeA)
check('A probe tell admitted', tell.ok, tell.status)
const tellA = await until(() => submitted('a').some((t) => t.includes('preflight-tell-arrives')))
check('A provisioned poll delivered the tell', tellA, submitted('a'))

const sendA = await layerA.hooks['session.send'](layerA.d, {
  to: 'avenor:' + probeA.runId,
  text: 'preflight-reply-through-provisioned-token',
  origin: { kind: 'model' },
})
check('A outbound through the provisioned token', Boolean(sendA) && sendA.isDelivered === true, sendA)

// The chain the name write used to break: a harness name outranks the record's
// own label, so an authorized rename would change a field peers never read.
const RENAMED_A = 'pf-child-a-renamed'
const renameA = await labelRun(provisionedA, RENAMED_A)
const labelAfterRename = await labelOf(provisionedA, RUN_A)
check('A an authorized rename moves the label peers see', labelAfterRename === RENAMED_A,
  { answered: renameA.label, listed: labelAfterRename })

// ---------- B. a run id without a token ----------
console.log('=== B. a pinned run id without a token still self-registers ===')
events.length = 0
const RUN_B = 'pf-top-' + STAMP
const NAME_B = 'pf-top-session'
const beforeB = await sessions(provisionedA)
const layerB = await loadLayer('b', {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: RUN_B,
  // The launch identity's own spelling, with no peer-credential copy beside it.
  AVENOR_LABEL: NAME_B,
})
await layerB.hooks['session.start'](layerB.d, { cwd: '/tmp', isInteractive: true })
const afterB = await sessions(provisionedA)
const rowB = afterB.sessions.find((s) => s.run_id === RUN_B)
check('B self-registered through /register', Boolean(rowB), rowB)
check('B issued exactly one /register', issued(layerB, '/register').length === 1, layerB.requests.map((r) => r.method + ' ' + r.path))
check('B posted the launch label it was handed', issued(layerB, '/session_name').length === 1,
  layerB.requests.map((r) => r.method + ' ' + r.path))
check('B is listed under that name', Boolean(rowB) && rowB.label === NAME_B, rowB)
check('B added one record', afterB.sessions.length === beforeB.sessions.length + 1,
  { before: beforeB.sessions.length, after: afterB.sessions.length })
const logsB = events.filter((e) => e.kind === 'b ui.log').map((e) => e.line).join('\n')
check('B logs the self-registered path', logsB.includes('peer layer on (self-registered run ' + RUN_B), logsB)

// ---------- C. no run id at all ----------
console.log('=== C. a bare session mints its own run and names it ===')
events.length = 0
const NAME_C = 'pf-bare-session'
const beforeC = await sessions(provisionedA)
const layerC = await loadLayer('c', {
  AVENOR_BROKER_URL: BROKER,
  // The older spelling a hand-exported shell may still set.
  AVENOR_SESSION_NAME: NAME_C,
})
await layerC.hooks['session.start'](layerC.d, { cwd: '/tmp', isInteractive: true })
const afterC = await sessions(provisionedA)
const freshC = afterC.sessions.find((s) => !beforeC.sessions.some((b) => b.run_id === s.run_id))
check('C minted one run', afterC.sessions.length === beforeC.sessions.length + 1 && Boolean(freshC),
  { before: beforeC.sessions.length, after: afterC.sessions.length })
check('C minted id is a claude-<hex>', Boolean(freshC) && /^claude-[0-9a-f]{6}$/.test(freshC.run_id), freshC && freshC.run_id)
check('C posted the name it was handed', issued(layerC, '/session_name').length === 1,
  layerC.requests.map((r) => r.method + ' ' + r.path))
check('C is listed under that name', Boolean(freshC) && freshC.label === NAME_C, freshC)

// ---------- D. one run, one inbound consumer ----------
console.log('=== D. a second session cannot join a run that already attached ===')
events.length = 0
// The same environment A was launched with: exactly what a nested session
// spawned from A's own shell inherits.
const layerD = await loadLayer('d', envA)
await layerD.hooks['session.start'](layerD.d, { cwd: '/tmp', isInteractive: true })
const logsD = events.filter((e) => e.kind === 'd ui.log').map((e) => e.line).join('\n')
check('D presented the spent credential once', issued(layerD, '/attach').length === 1,
  layerD.requests.map((r) => r.method + ' ' + r.path))
check('D disabled itself with the broker\'s reason',
  logsD.includes('refused this launch\'s attach') && logsD.includes('already used'), logsD)
check('D polled nothing', issued(layerD, '/poll-control').length === 0,
  layerD.requests.map((r) => r.method + ' ' + r.path))

// The refusal is about the second consumer, not the run: A still serves its
// queue, which is the observable difference between one consumer and two.
const tellDId = 'av1.preflight.d.' + STAMP
const tellD = await post('/send', {
  from_run_id: probeA.runId,
  to_run_id: RUN_A,
  type: 'agent_message',
  message_id: tellDId,
  payload: {
    id: tellDId,
    from: probeA.runId,
    from_run_id: probeA.runId,
    to_run_id: RUN_A,
    message: 'preflight-single-consumer',
    role: 'agent',
  },
}, probeA)
check('D probe tell admitted', tellD.ok, tellD.status)
const tellAfterD = await until(() => submitted('a').some((t) => t.includes('preflight-single-consumer')))
check('the tell still reaches A, the only consumer', tellAfterD, submitted('a'))

console.log(failures === 0 ? 'PREFLIGHT OK' : 'PREFLIGHT FAILED (' + failures + ')')
process.exit(failures === 0 ? 0 : 1)

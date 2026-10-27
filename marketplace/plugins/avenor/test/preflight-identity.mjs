// Register-identity preflight for the avenor peer layer (issue #391, PR 2).
// Loads the real hooks/register.js against a stub engine-call object whose http
// noun speaks to a REAL broker, and drives session.start once per identity the
// layer can be handed:
//
//   A. provisioned — broker URL, run id and token name a run the launcher
//      already holds, and AVENOR_SESSION_NAME carries the launch label (every
//      supervisor spawn passes one). The layer joins that run and writes
//      nothing about it: no /register, no /session_name. The name write is
//      asserted absent by path — a harness name outranks the record's label in
//      the broker, so posting the launch label would shadow the name peers
//      address the run by, including through a later authorized rename.
//   B. half-provisioned — a run id without a token. The layer self-registers
//      through the broker's open /register route and posts the name it was
//      handed.
//   C. bare — no run id at all. The layer mints its own run and posts its name.
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
// through /rename, which is the route an authorized name change takes.
const STAMP = Date.now().toString(36)
async function provisionRun(runId, token) {
  const res = await post('/register', { run_id: runId, token })
  if (!res.ok) throw new Error('provision ' + runId + ': ' + res.status + ' ' + res.text)
  return { runId, token }
}
async function labelRun(creds, name) {
  const res = await post('/rename', { name }, creds)
  if (!res.ok) throw new Error('rename ' + creds.runId + ': ' + res.status + ' ' + res.text)
  return JSON.parse(res.text)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

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

const layerA = await loadLayer('a', {
  AVENOR_BROKER_URL: BROKER,
  AVENOR_RUN_ID: RUN_A,
  AVENOR_BROKER_TOKEN: 'preflight-provisioned-token',
  // The launch label, exactly as a supervisor spawn passes it.
  AVENOR_SESSION_NAME: NAME_A,
})
await layerA.hooks['session.start'](layerA.d, { cwd: '/tmp', isInteractive: true })

const afterA = await sessions(provisionedA)
const rowA = afterA.sessions.find((s) => s.run_id === RUN_A)
check('A joined the provisioned run as its launch label', Boolean(rowA) && rowA.label === NAME_A, rowA)
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
await sleep(3000)
const submittedA = events.filter((e) => e.kind === 'a prompt.submit').map((e) => e.line)
check('A provisioned poll delivered the tell', submittedA.some((t) => t.includes('preflight-tell-arrives')), submittedA)

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
  AVENOR_SESSION_NAME: NAME_B,
})
await layerB.hooks['session.start'](layerB.d, { cwd: '/tmp', isInteractive: true })
const afterB = await sessions(provisionedA)
const rowB = afterB.sessions.find((s) => s.run_id === RUN_B)
check('B self-registered through /register', Boolean(rowB), rowB)
check('B issued exactly one /register', issued(layerB, '/register').length === 1, layerB.requests.map((r) => r.method + ' ' + r.path))
check('B posted the name it was handed', issued(layerB, '/session_name').length === 1,
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

console.log(failures === 0 ? 'PREFLIGHT OK' : 'PREFLIGHT FAILED (' + failures + ')')
process.exit(failures === 0 ? 0 : 1)

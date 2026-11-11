import { afterAll, afterEach, beforeAll, expect, test } from 'bun:test'
import { execFileSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { startExtensionHarness, waitFor } from './test-fixtures.js'

let root = ''
let repo = ''
let plain = ''

beforeAll(() => {
  root = mkdtempSync(join(tmpdir(), 'avenor-default-label-'))
  repo = join(root, 'demo-repo')
  mkdirSync(repo)
  execFileSync('git', ['init', '-q', '-b', 'feat-x', repo])
  plain = join(root, 'plain-dir')
  mkdirSync(plain)
})

afterAll(() => rmSync(root, { recursive: true }))

const previousFetch = globalThis.fetch
const previousEnv = { name: process.env.AVENOR_SESSION_NAME, label: process.env.AVENOR_LABEL }
afterEach(() => {
  globalThis.fetch = previousFetch
  for (const [key, value] of [['AVENOR_SESSION_NAME', previousEnv.name], ['AVENOR_LABEL', previousEnv.label]] as const) {
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
})

function broker(heldLabels: string[] = [], sessions?: () => Response | Promise<Response>) {
  const calls: Array<{ path: string; body: Record<string, unknown> }> = []
  globalThis.fetch = Object.assign(async (url: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(url)).pathname
    const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
    calls.push({ path, body })
    if (path === '/register') return Response.json(body)
    if (path === '/sessions') {
      if (sessions) return sessions()
      return Response.json({
        generation: 'gen',
        sessions: [
          { run_id: 'other', label: heldLabels[0] ?? 'unrelated', status: 'running' },
          ...heldLabels.slice(1).map((label, index) => ({ run_id: `other-${index}`, label, status: 'running' })),
        ],
      })
    }
    if (path === '/poll-control') return Response.json([])
    if (path === '/rename') return Response.json({ run_id: body.run_id, label: body.name, previous_label: '' })
    return Response.json({})
  }, { preconnect() {} }) as typeof fetch
  return calls
}

async function startTopLevel(sessionName?: () => string | undefined, cwd = repo) {
  return startExtensionHarness({ cwd, sessionName, extOptions: { topLevelBrokerUrl: 'http://127.0.0.1:8787' } })
}

// The run id every top-level session in these tests registers under.
async function ownTail(calls: ReturnType<typeof broker>): Promise<string> {
  await waitFor(() => calls.some(call => call.path === '/register'))
  return String(calls.find(call => call.path === '/register')!.body.run_id).replace(/[^A-Za-z0-9]/g, '').slice(-6)
}

test('an unnamed top-level session registers under its repository through /rename', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    const rename = calls.find(call => call.path === '/rename')!
    // The harness-derived label says so, so a broker that checks names makes it
    // unique instead of refusing it; run_id/token are the ordinary credentials.
    expect(rename.body).toEqual({
      session_id: '',
      name: 'pi-demo-repo',
      default: true,
      run_id: expect.any(String),
      token: expect.any(String),
    })
    // /session_name is the human-name slot and would outrank a later rename.
    expect(calls.filter(call => call.path === '/session_name').map(call => call.body.name)).not.toContain('pi-demo-repo')
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a label another peer holds gains the run id tail', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker(['pi-demo-repo'])
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    expect(calls.find(call => call.path === '/rename')!.body.name).toBe(`pi-demo-repo-${await ownTail(calls)}`)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a directory outside any repository stands in for the repo and gains the run id tail', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startTopLevel(undefined, plain)
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    expect(calls.find(call => call.path === '/rename')!.body.name).toBe(`pi-plain-dir-${await ownTail(calls)}`)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a broker on another machine leads the label with the machine name', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startExtensionHarness({
    cwd: repo,
    extOptions: { topLevelBrokerUrl: 'https://broker.test', hostname: () => 'fenwick.home.example.com' },
  })
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    expect(calls.find(call => call.path === '/rename')!.body.name).toBe('fenwick-pi-demo-repo')
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a broker addressed by this machine\'s own name adds no prefix', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startExtensionHarness({
    cwd: repo,
    extOptions: { topLevelBrokerUrl: 'https://fenwick.home.example.com', hostname: () => 'fenwick' },
  })
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    expect(calls.find(call => call.path === '/rename')!.body.name).toBe('pi-demo-repo')
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a Pi session name is published as the human name and no default label is derived', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startTopLevel(() => 'my-session')
  try {
    await waitFor(() => calls.some(call => call.path === '/session_name' && call.body.name === 'my-session'))
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/rename')).toBe(false)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('an environment name is published as before and no default label is derived', async () => {
  process.env.AVENOR_SESSION_NAME = 'from-env'
  const calls = broker()
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/session_name' && call.body.name === 'from-env'))
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/rename')).toBe(false)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('an AVENOR_LABEL environment name derives no default label', async () => {
  delete process.env.AVENOR_SESSION_NAME
  process.env.AVENOR_LABEL = 'label-from-env'
  const calls = broker()
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/rename')).toBe(false)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a peers read that fails leaves registration intact and the label unset', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  let sessionReads = 0
  const calls = broker([], () => (++sessionReads === 1 ? new Response('boom', { status: 500 }) : Response.json({ generation: 'gen', sessions: [] })))
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/register')).toBe(true)
    expect(calls.some(call => call.path === '/rename')).toBe(false)
    // A registration marked failed would reject every tool call.
    expect((await h.registeredTools.avenor_peers.execute('p', {})).details.peers).toEqual([])
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('an incomplete peers listing gains the run id tail', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker([], () => Response.json({
    generation: 'gen',
    sessions: [{ run_id: 'other', label: 'unrelated', status: 'running' }],
    peers_error: 'one host did not answer',
  }))
  const h = await startTopLevel()
  try {
    await waitFor(() => calls.some(call => call.path === '/rename'))
    expect(calls.find(call => call.path === '/rename')!.body.name).toBe(`pi-demo-repo-${await ownTail(calls)}`)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a Pi session name arriving during the peers read wins over the derived label', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  let name: string | undefined
  let release: (response: Response) => void = () => {}
  const pending = new Promise<Response>(resolve => { release = resolve })
  const calls = broker([], () => pending)
  // session_start awaits registration, which is parked on the peers read.
  const started = startTopLevel(() => name)
  await waitFor(() => calls.some(call => call.path === '/sessions'))
  name = 'chosen-late'
  release(Response.json({ generation: 'gen', sessions: [] }))
  const h = await started
  try {
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/rename')).toBe(false)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

test('a sub-agent child derives no default label', async () => {
  delete process.env.AVENOR_SESSION_NAME
  delete process.env.AVENOR_LABEL
  const calls = broker()
  const h = await startExtensionHarness({
    cwd: repo,
    extOptions: { subAgentEnv: { brokerUrl: 'https://broker.test', runId: 'rt_child', token: 'tok' } },
  })
  try {
    await waitFor(() => calls.some(call => call.path === '/poll-control'))
    expect(calls.some(call => call.path === '/rename' || call.path === '/register')).toBe(false)
  } finally { await h.eventHandlers.session_shutdown({}, {}) }
})

import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { describe, expect, test } from 'bun:test'

import { askWebhook, runBridge } from './run.js'
import { decisionResponseHash } from './command.js'
import type { ControlClient, WorkflowDetail } from './types.js'

function parkedDetail(): WorkflowDetail {
  return {
    instance: { status: 'active' },
    activations: [
      {
        activation_id: 'act_1',
        node_id: 'merge-auth',
        status: 'awaiting_gate',
        selected_outcome: 'authorized',
      },
    ],
    gates: null,
    outputs: [{ definition_id: 'head_sha', revision: 2, value: '"abc123"' }],
  }
}

function mergeAuthGate() {
  return {
    id: 'merge-authorization',
    name: 'Merge authorization',
    type: 'human',
    required: true,
    subject_type: 'pull_request',
    allowed_outcomes: ['authorized'],
  }
}

function templateFile(dir: string): string {
  const templatePath = path.join(dir, 'template.json')
  fs.writeFileSync(
    templatePath,
    JSON.stringify({ nodes: [{ id: 'merge-auth', gates: [mergeAuthGate()] }] }),
  )
  return templatePath
}

interface FakeClientOptions {
  detail: WorkflowDetail
  waitResults: unknown[]
}

class FakeClient implements ControlClient {
  calls: Array<{ method: string; params?: unknown }> = []

  constructor(private opts: FakeClientOptions) {}

  async call(method: string, params?: unknown): Promise<unknown> {
    this.calls.push({ method, params })
    if (method === 'workflow.wait') return this.opts.waitResults.shift()
    if (method === 'workflow.inspect') return this.opts.detail
    if (method === 'workflow.command') return {}
    throw new Error(`unexpected method ${method}`)
  }

  commands() {
    return this.calls.filter((c) => c.method === 'workflow.command').map((c) => c.params!.command)
  }

  isClosed(): boolean {
    return false
  }

  close(): void {}
}

function setupDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'escalation-bridge-'))
}

describe('askWebhook', () => {
  test('refuses redirects, surfaces the non-OK status, and POSTs JSON', async () => {
    let received: { method?: string; contentType?: string; body?: string } | undefined
    let redirectTargetHits = 0
    let base = ''
    const server = Bun.serve({
      port: 0,
      fetch: async (req) => {
        const url = new URL(req.url)
        if (url.pathname === '/redirect') {
          // A redirecting endpoint would receive the question payload; the
          // client must refuse to follow rather than forward it. The
          // location is a live endpoint on this server so a client that
          // followed the redirect would be observable (a follow + connection
          // failure to a dead port is not proof the redirect was refused).
          return new Response(null, {
            status: 302,
            headers: { location: `${base}/elsewhere` },
          })
        }
        if (url.pathname === '/elsewhere') {
          redirectTargetHits += 1
          return new Response('ok', { status: 200 })
        }
        if (url.pathname === '/fail') return new Response('nope', { status: 500 })
        received = {
          method: req.method,
          contentType: req.headers.get('content-type'),
          body: await req.text(),
        }
        return new Response('ok', { status: 200 })
      },
    })
    base = `http://127.0.0.1:${server.port}`

    await expect(askWebhook(`${base}/redirect`, { q: 1 })).rejects.toThrow(Error)
    expect(redirectTargetHits).toBe(0)

    const err = await askWebhook(`${base}/fail`, { q: 1 }).catch((e: Error) => e)
    expect(err.message).toContain('500')

    await askWebhook(`${base}/ok`, { q: 1 })
    expect(received?.method).toBe('POST')
    expect(received?.contentType).toBe('application/json')
    expect(JSON.parse(received!.body)).toEqual({ q: 1 })

    server.stop()
  })

  test('aborts when the endpoint accepts the connection but never responds', async () => {
    let requests = 0
    const server = Bun.serve({
      port: 0,
      fetch: async () => {
        requests += 1
        // Long past the injected timeout; only the abort path can release
        // the caller (the default is 10s, far too slow for a test).
        await new Promise((resolve) => setTimeout(resolve, 2000))
        return new Response('late', { status: 200 })
      },
    })
    try {
      await expect(
        askWebhook(`http://127.0.0.1:${server.port}/slow`, { q: 1 }, { timeoutMs: 50 }),
      ).rejects.toThrow(/timed out/)
      expect(requests).toBe(1)
    } finally {
      server.stop()
    }
  })
})

describe('runBridge', () => {
  test('asks, records the decision, and exits when the workflow turns terminal', async () => {
    const dir = setupDir()
    const decision = {
      decision: 'satisfy',
      actor: 'austin',
      reason: 'Diff reviewed; approved',
      evidence: 'https://slack.com/archives/C123/p1650',
      subject: { type: 'pull_request', repository: 'org/repo', pull_request: 123, revision: 'abc123' },
    }
    fs.writeFileSync(
      path.join(dir, 'decision-act_1-merge-authorization.json'),
      JSON.stringify(decision),
    )
    const client = new FakeClient({
      detail: parkedDetail(),
      waitResults: [{ terminal: false }, { terminal: true, instance: { status: 'completed', terminal_outcome: 'merged' } }],
    })
    const asks: unknown[] = []
    const logs: string[] = []

    const code = await runBridge({
      socketPath: '/tmp/does-not-matter.sock',
      workflowId: 'wf_1',
      webhookUrl: 'http://127.0.0.1:1/ask',
      decisionDir: dir,
      templatePath: templateFile(dir),
      connect: async () => client,
      ask: async (_url, payload) => {
        asks.push(payload)
      },
      sleep: async () => {},
      log: (message) => logs.push(message),
    })

    expect(code).toBe(0)
    // One ask carrying the question payload built from the inspect snapshot.
    expect(asks).toHaveLength(1)
    const question = asks[0] as Record<string, unknown>
    expect(question.gate_id).toBe('merge-authorization')
    expect(question.decision_file).toBe('decision-act_1-merge-authorization.json')
    expect(question.outputs).toEqual({ head_sha: 'abc123' })
    // The decision was recorded through the ordinary gate command path.
    const commands = client.commands()
    expect(commands).toHaveLength(1)
    const command = commands[0] as Record<string, unknown>
    expect(command.operation).toBe('satisfy')
    expect(command.response_hash).toBe(decisionResponseHash(decision))
    expect((command.evidence_ids as string[])[0]).toBe(`ev_bridge_${command.response_hash}`)
    // The answered file was archived only after the kernel accepted it.
    const recorded = fs.readdirSync(path.join(dir, 'recorded'))
    expect(recorded).toEqual([`act_1-merge-authorization-${command.response_hash}.json`])
    expect(fs.existsSync(path.join(dir, 'decision-act_1-merge-authorization.json'))).toBe(false)
    expect(logs.some((l) => l.includes('workflow terminal'))).toBe(true)
    expect(logs.some((l) => l.includes('outcome merged'))).toBe(true)
  })

  test('parks an invalid decision under rejected/ and carries the error on the next ask', async () => {
    const dir = setupDir()
    fs.writeFileSync(
      path.join(dir, 'decision-act_1-merge-authorization.json'),
      JSON.stringify({ decision: 'approve', actor: 'austin', reason: 'ok' }),
    )
    const client = new FakeClient({
      detail: parkedDetail(),
      // Two non-terminal ticks so the gate is asked twice; the third tick is
      // terminal and ends the run.
      waitResults: [
        { terminal: false },
        { terminal: false },
        { terminal: true, instance: { status: 'completed' } },
      ],
    })
    const asks: Array<Record<string, unknown>> = []

    const code = await runBridge({
      socketPath: '/tmp/does-not-matter.sock',
      workflowId: 'wf_1',
      webhookUrl: 'http://127.0.0.1:1/ask',
      decisionDir: dir,
      templatePath: templateFile(dir),
      // Short gate timeout: the first poll iteration reads the invalid file
      // and parks it; the second ask's wait expires quickly.
      gateTimeoutMs: 10,
      connect: async () => client,
      ask: async (_url, payload) => {
        asks.push(payload as Record<string, unknown>)
      },
      sleep: async () => {},
      log: () => {},
    })

    expect(code).toBe(0)
    expect(client.commands()).toHaveLength(0)
    expect(asks).toHaveLength(2)
    expect(asks[0].previous_decision_error).toBeUndefined()
    expect(asks[1].previous_decision_error).toContain('satisfy')
    expect(fs.readdirSync(path.join(dir, 'rejected'))).toHaveLength(1)
  })

  test('leaves the decision file in place when the kernel rejects the command', async () => {
    // Archive must happen only after the kernel accepted the record; a failed
    // command must not consume the human's answer.
    const dir = setupDir()
    const decision = {
      decision: 'satisfy',
      actor: 'austin',
      reason: 'ok',
      subject: { type: 'pull_request', repository: 'org/repo', pull_request: 123, revision: 'abc' },
    }
    fs.writeFileSync(
      path.join(dir, 'decision-act_1-merge-authorization.json'),
      JSON.stringify(decision),
    )
    const detail = parkedDetail()
    const waitResults = [
      { terminal: false },
      { terminal: false },
      { terminal: true, instance: { status: 'completed' } },
    ]
    const calls: Array<{ method: string; params?: unknown }> = []
    const client: ControlClient = {
      async call(method, params) {
        calls.push({ method, params })
        if (method === 'workflow.wait') return waitResults.shift()
        if (method === 'workflow.inspect') return detail
        if (method === 'workflow.command') {
          commandAttempts++
          if (commandAttempts === 1) throw new Error('kernel unavailable')
          return {}
        }
        throw new Error(`unexpected method ${method}`)
      },
      isClosed: () => false,
      close: () => {},
    }
    let commandAttempts = 0
    const asks: unknown[] = []

    const code = await runBridge({
      socketPath: '/tmp/does-not-matter.sock',
      workflowId: 'wf_1',
      webhookUrl: 'http://127.0.0.1:1/ask',
      decisionDir: dir,
      templatePath: templateFile(dir),
      connect: async () => client,
      ask: async (_url, payload) => {
        asks.push(payload)
      },
      sleep: async () => {},
      log: () => {},
    })

    expect(code).toBe(0)
    // The failed record was retried on the next tick and the answer was not
    // consumed by the failure.
    expect(commandAttempts).toBe(2)
    expect(asks).toHaveLength(2)
    expect(fs.readdirSync(path.join(dir, 'recorded'))).toHaveLength(1)
    expect(fs.existsSync(path.join(dir, 'decision-act_1-merge-authorization.json'))).toBe(false)
  })

  test('reconnects and backs off after a transient connect failure', async () => {
    const dir = setupDir()
    const client = new FakeClient({
      detail: parkedDetail(),
      waitResults: [{ terminal: true, instance: { status: 'completed' } }],
    })
    let connectCalls = 0
    const sleeps: number[] = []

    const code = await runBridge({
      socketPath: '/tmp/does-not-matter.sock',
      workflowId: 'wf_1',
      webhookUrl: 'http://127.0.0.1:1/ask',
      decisionDir: dir,
      templatePath: templateFile(dir),
      connect: async () => {
        connectCalls++
        if (connectCalls === 1) throw new Error('socket gone')
        return client
      },
      ask: async () => {},
      sleep: async (ms) => {
        sleeps.push(ms)
      },
      log: () => {},
    })

    expect(code).toBe(0)
    expect(connectCalls).toBe(2)
    expect(sleeps).toContain(5000)
  })

  const runOptions = (dir: string, overrides: Record<string, unknown> = {}) => ({
    socketPath: '/tmp/does-not-matter.sock',
    workflowId: 'wf_1',
    webhookUrl: 'http://127.0.0.1:1/ask',
    decisionDir: dir,
    // Tests that pin a templatePath (missing/malformed/traversal/external)
    // must not have the default template written over it.
    templatePath:
      'templatePath' in overrides ? (overrides.templatePath as string) : templateFile(dir),
    sleep: async () => {},
    log: () => {},
    ...overrides,
  })

  test('floors the decision poll at one second', async () => {
    const dir = setupDir()
    const client = new FakeClient({
      detail: parkedDetail(),
      waitResults: [{ terminal: false }, { terminal: true, instance: { status: 'completed' } }],
    })
    const sleeps: number[] = []
    // No decision file ever appears, so waitForDecision runs its poll loop to
    // the gate timeout and every recorded sleep is a poll interval: with the
    // scripted clock, `pollMs: 100` must be floored to the Python reference's
    // 1.0s (deadline 2500ms / three 1000ms ticks).
    const scripted = [0, 0, 1000, 2000, 3000]
    const now = () => (scripted.length > 1 ? scripted.shift()! : scripted[0])
    const code = await runBridge(
      runOptions(dir, {
        pollMs: 100,
        gateTimeoutMs: 2500,
        connect: async () => client,
        ask: async () => {},
        sleep: async (ms) => {
          sleeps.push(ms)
        },
        now,
      }),
    )
    expect(code).toBe(0)
    expect(sleeps.length).toBeGreaterThan(0)
    expect(sleeps.every((ms) => ms === 1000)).toBe(true)
  })

  test('creates the decision dir 0700', async () => {
    const parent = setupDir()
    const dir = path.join(parent, 'decisions')
    // The template lives outside the decision dir so the dir does not exist
    // before runBridge; the assertion pins the 0700 creation itself.
    const decision = {
      decision: 'satisfy',
      actor: 'austin',
      reason: 'ok',
      subject: { type: 'pull_request', repository: 'org/repo', pull_request: 123, revision: 'abc123' },
    }
    const client = new FakeClient({
      detail: parkedDetail(),
      waitResults: [{ terminal: false }, { terminal: true, instance: { status: 'completed' } }],
    })
    const code = await runBridge(
      runOptions(dir, {
        templatePath: templateFile(parent),
        connect: async () => client,
        ask: async () => {
          fs.writeFileSync(
            path.join(dir, 'decision-act_1-merge-authorization.json'),
            JSON.stringify(decision),
          )
        },
      }),
    )
    expect(code).toBe(0)
    expect(fs.statSync(dir).mode & 0o777).toBe(0o700)
  })

  test('refuses a group- or other-writable pre-existing decision dir', async () => {
    const dir = setupDir()
    fs.chmodSync(dir, 0o755)
    const client = new FakeClient({ detail: parkedDetail(), waitResults: [] })
    await expect(
      runBridge(runOptions(dir, { connect: async () => client, ask: async () => {} })),
    ).rejects.toThrow(/must not be group- or other-writable/)
  })

  test('rejects a missing or malformed template before the loop', async () => {
    const missingDir = setupDir()
    const badDir = setupDir()
    fs.writeFileSync(path.join(badDir, 'template.json'), 'not json')
    const client = new FakeClient({ detail: parkedDetail(), waitResults: [] })
    await expect(
      runBridge({
        ...runOptions(missingDir),
        templatePath: path.join(missingDir, 'nope.json'),
        connect: async () => client,
        ask: async () => {},
      }),
    ).rejects.toThrow(/ENOENT/)
    // runOptions skips the fixture write when templatePath is overridden, so
    // the malformed template survives to be read.
    await expect(
      runBridge(runOptions(badDir, {
        templatePath: path.join(badDir, 'template.json'),
        connect: async () => client,
        ask: async () => {},
      })),
    ).rejects.toThrow(/JSON Parse error/)
    expect(fs.readFileSync(path.join(badDir, 'template.json'), 'utf8')).toBe('not json')
  })

  test('rejects traversal-prone gate ids before the loop', async () => {
    const dir = setupDir()
    const templatePath = path.join(dir, 'template.json')
    fs.writeFileSync(
      templatePath,
      JSON.stringify({
        nodes: [{ id: 'merge-auth', gates: [{ ...mergeAuthGate(), id: '../../evil' }] }],
      }),
    )
    const client = new FakeClient({ detail: parkedDetail(), waitResults: [] })
    await expect(
      runBridge(runOptions(dir, {
        templatePath,
        connect: async () => client,
        ask: async () => {},
      })),
    ).rejects.toThrow(/unsafe gate id/)
  })

  test('stops asking once waitForDecision sees a terminal workflow', async () => {
    // The run-level consequence of waitForDecision's 'terminal' reason: the
    // per-tick re-check observes the workflow end and the bridge must break
    // out of the gate loop instead of re-asking the transport on the next
    // tick. wait.test.ts covers the reason itself; this covers the break.
    const dir = setupDir()
    const parked = parkedDetail()
    const terminalDetail: WorkflowDetail = {
      instance: { status: 'completed', terminal_outcome: 'merged' },
      activations: parked.activations,
      gates: null,
      outputs: [],
    }
    const waitResults = [
      { terminal: false },
      { terminal: true, instance: { status: 'completed', terminal_outcome: 'merged' } },
    ]
    const calls: Array<{ method: string; params?: unknown }> = []
    let inspects = 0
    const client: ControlClient = {
      async call(method, params) {
        calls.push({ method, params })
        if (method === 'workflow.wait') return waitResults.shift()
        if (method === 'workflow.inspect') {
          inspects += 1
          // First inspect (after the ask) still sees the parked gate; the
          // second one (inside waitForDecision's re-check) sees terminal.
          return inspects === 1 ? parked : terminalDetail
        }
        if (method === 'workflow.command') return {}
        throw new Error(`unexpected method ${method}`)
      },
      isClosed: () => false,
      close: () => {},
    }
    const asks: unknown[] = []

    const code = await runBridge({
      socketPath: '/tmp/does-not-matter.sock',
      workflowId: 'wf_1',
      webhookUrl: 'http://127.0.0.1:1/ask',
      decisionDir: dir,
      templatePath: templateFile(dir),
      connect: async () => client,
      ask: async (_url, payload) => {
        asks.push(payload)
      },
      sleep: async () => {},
      log: () => {},
    })

    expect(code).toBe(0)
    // Exactly one ask: after the terminal re-check the gate loop broke and
    // the next workflow.wait ended the run — no second question was sent for
    // a workflow that can no longer answer.
    expect(asks).toHaveLength(1)
    expect(calls.filter((c) => c.method === 'workflow.command')).toHaveLength(0)
  })
})

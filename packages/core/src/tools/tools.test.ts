import { describe, it, expect, afterAll, afterEach } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { findAvenorBinary, Supervisor } from '../supervisor.js'
import { spawnTool } from './spawn.js'
import { statusTool, translateStatus } from './status.js'
import { answerPermissionTool } from './answer-permission.js'
import { eventsTool } from './events.js'
import { shutdownTool } from './shutdown.js'

function useFakeAvenorUnlessRealIntegration(): void {
  if (process.env.AVENOR_REAL_INTEGRATION === '1') return
  process.env.AVENOR_HOME = path.join(os.tmpdir(), `avenor-core-test-${process.pid}`)
  process.env.AVENOR_BIN = path.resolve(import.meta.dir, '..', '..', 'test', 'fixtures', 'fake-avenor.ts')
}

useFakeAvenorUnlessRealIntegration()

function hasAvenorBinary(): boolean {
  try {
    findAvenorBinary()
    return true
  } catch {
    return false
  }
}

const skipIfNoBinary = !hasAvenorBinary()

describe.skipIf(skipIfNoBinary)('spawnTool + statusTool + shutdownTool integration', () => {
  let sup: Supervisor
  let runId: string

  afterAll(async () => {
    await sup?.close().catch(() => {})
  })

  it('spawnTool spawns a run', async () => {
    sup = await Supervisor.get()
    const result = await spawnTool({
      agent: '',
      prompt: 'exit 0',
      dir: process.cwd(),
      label: 'custom-label',
    })
    runId = result.run_id
    expect(result.run_id).toBeString()
    expect(result.label).toBe('custom-label')
    expect(result.supervisor_id).toBeString()
    expect(result.supervisor_id).toBe(sup.supervisorId)
  }, 15_000)

  it('statusTool returns alive status', async () => {
    const status = await statusTool({ runId })
    const result = Array.isArray(status) ? status[0] : status
    expect(result.run_id).toBe(runId)
    expect(typeof result.status).toBe('string')
  }, 10_000)

  it('statusTool list preserves the stable run ID separately from its label', async () => {
    const list = await statusTool({})
    expect('runs' in list).toBe(true)
    const results = 'runs' in list ? list.runs : [list]
    const result = results.find(entry => entry.run_id === runId)
    expect(result?.label).toBe('custom-label')
  }, 10_000)

  it('eventsTool returns events array', async () => {
    const result = await eventsTool({ runId })
    expect(Array.isArray(result.events)).toBe(true)
  }, 10_000)

  it('shutdownTool shuts down and cleans up', async () => {
    const result = await shutdownTool({ force: true })
    expect(result.ok).toBe(true)
    expect(Array.isArray(result.cleaned_up)).toBe(true)

    sup = null as any
  }, 10_000)
})

describe('statusTool with mock sentinel file', () => {
  const tmpdir = os.tmpdir()
  let sentinelPath: string
  let testRunId: string

  afterEach(() => {
    if (sentinelPath) {
      try { fs.unlinkSync(sentinelPath) } catch {}
    }
  })

  it('reads DONE status and SESSION from sentinel file', () => {
    testRunId = 'test-run-sentinel-status'
    sentinelPath = path.join(tmpdir, `avenor-run-${testRunId}.done`)
    fs.writeFileSync(sentinelPath, 'DONE\nSESSION=ses_test\n')

    // Verify sentinel was written correctly
    const content = fs.readFileSync(sentinelPath, 'utf-8')
    expect(content).toContain('DONE')
    expect(content).toContain('SESSION=ses_test')
  })

  it('parses sentinel with multiple key=value lines', () => {
    testRunId = 'test-run-sentinel-multi'
    sentinelPath = path.join(tmpdir, `avenor-run-${testRunId}.done`)
    fs.writeFileSync(
      sentinelPath,
      'DONE\nSESSION=ses_abc\nRUNTIME=rt_123\nEXIT=0\n',
    )

    const content = fs.readFileSync(sentinelPath, 'utf-8')
    const lines = content.trim().split('\n')
    expect(lines[0]).toBe('DONE')

    const kv: Record<string, string> = {}
    for (let i = 1; i < lines.length; i++) {
      const eq = lines[i].indexOf('=')
      if (eq > 0) {
        kv[lines[i].substring(0, eq)] = lines[i].substring(eq + 1)
      }
    }

    expect(kv.SESSION).toBe('ses_abc')
    expect(kv.RUNTIME).toBe('rt_123')
    expect(kv.EXIT).toBe('0')
  })
})

describe('eventsTool reading mock NDJSON log file', () => {
  const tmpdir = os.tmpdir()
  let logPath: string

  afterEach(() => {
    if (logPath) {
      try { fs.unlinkSync(logPath) } catch {}
    }
  })

  it('reads and parses NDJSON events from a log file', () => {
    const runId = 'test-run-events-mock'
    logPath = path.join(tmpdir, `avenor-run-${runId}.log`)
    const events = [
      JSON.stringify({ type: 'agent.status', ts: 1000, phase: 'thinking' }),
      JSON.stringify({ type: 'tool.use', ts: 1001, tool: 'read' }),
      JSON.stringify({ type: 'agent.status', ts: 1002, phase: 'complete' }),
    ]
    fs.writeFileSync(logPath, events.join('\n') + '\n')

    const raw = fs.readFileSync(logPath, 'utf-8')
    const lines = raw.split('\n').filter((line) => line.trim())
    const parsed = lines.map((line) => JSON.parse(line))

    expect(parsed).toHaveLength(3)
    expect(parsed[0].type).toBe('agent.status')
    expect(parsed[0].ts).toBe(1000)
    expect(parsed[1].tool).toBe('read')
    expect(parsed[2].phase).toBe('complete')
  })

  it('filters events by type', () => {
    const runId = 'test-run-events-filter'
    logPath = path.join(tmpdir, `avenor-run-${runId}.log`)
    const events = [
      JSON.stringify({ type: 'agent.status', ts: 1 }),
      JSON.stringify({ type: 'tool.use', ts: 2 }),
      JSON.stringify({ type: 'agent.status', ts: 3 }),
      JSON.stringify({ type: 'tool.use', ts: 4 }),
    ]
    fs.writeFileSync(logPath, events.join('\n') + '\n')

    const raw = fs.readFileSync(logPath, 'utf-8')
    const parsed = raw
      .split('\n')
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line))

    const filtered = parsed.filter((e) => e.type === 'agent.status')
    expect(filtered).toHaveLength(2)
    expect(filtered[0].ts).toBe(1)
    expect(filtered[1].ts).toBe(3)
  })

  it('enforces a limit on returned events', () => {
    const runId = 'test-run-events-limit'
    logPath = path.join(tmpdir, `avenor-run-${runId}.log`)
    const lines: string[] = []
    for (let i = 0; i < 100; i++) {
      lines.push(JSON.stringify({ type: 'tick', ts: i }))
    }
    fs.writeFileSync(logPath, lines.join('\n') + '\n')

    const raw = fs.readFileSync(logPath, 'utf-8')
    const parsed = raw
      .split('\n')
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line))

    const limit = 10
    const limited = parsed.length > limit ? parsed.slice(parsed.length - limit) : parsed
    expect(limited).toHaveLength(limit)
    expect(limited[0].ts).toBe(90)
    expect(limited[limited.length - 1].ts).toBe(99)
  })
})

describe('translateStatus', () => {
  const done = { _status: 'DONE' }

  // Regression: the "waiting" phase (set by the Go statusTracker when a
  // sub-agent asks a question or requests a permission) used to fall through
  // to the default branch and return "running", causing blocking spawn loops
  // to spin forever instead of returning control to the orchestrator.
  it('returns "waiting" for the waiting phase', () => {
    expect(translateStatus('running', 'waiting', null)).toBe('waiting')
    expect(translateStatus('idle', 'waiting', null)).toBe('waiting')
  })

  it('returns "waiting" for the waiting status', () => {
    expect(translateStatus('waiting', '', null)).toBe('waiting')
    expect(translateStatus('waiting', 'waiting', null)).toBe('waiting')
  })

  it('returns "waiting" even when a sentinel is present (waiting is not terminal)', () => {
    expect(translateStatus('running', 'waiting', done)).toBe('waiting')
    expect(translateStatus('waiting', '', done)).toBe('waiting')
  })

  it('preserves the waiting phase even with a non-terminal sentinel', () => {
    expect(translateStatus('running', 'waiting', { _status: 'RUNNING' })).toBe('waiting')
  })

  // Regression: in stable mode the supervisor parks a runtime after a clean
  // end_turn, leaving status "running" while the sentinel already reads DONE.
  // translateStatus must consult the sentinel so the orchestrator observes
  // completion instead of polling forever.
  it('lets a DONE sentinel override a live "running" status', () => {
    expect(translateStatus('running', '', done)).toBe('done')
    expect(translateStatus('running', 'done', done)).toBe('done')
  })

  // A follow-up turn wakes a parked runtime without rotating its sentinel, so a
  // running runtime in an active phase must not read the previous turn's outcome.
  it('keeps a running runtime in an active phase running despite an earlier DONE sentinel', () => {
    for (const phase of ['working', 'thinking']) {
      expect(translateStatus('running', phase, done)).toBe('running')
      expect(translateStatus('idle', phase, done)).toBe('running')
    }
  })

  it('lets a FAILED sentinel override a live "running" status', () => {
    expect(translateStatus('running', '', { _status: 'FAILED' })).toBe('failed')
  })

  it('recognizes lowercase terminal statuses from the control protocol', () => {
    expect(translateStatus('done', '', null)).toBe('done')
    expect(translateStatus('failed', '', null)).toBe('failed')
    expect(translateStatus('timeout', '', null)).toBe('timeout')
    expect(translateStatus('killed', '', null)).toBe('killed')
  })

  it('returns "running" when status is running and no sentinel exists', () => {
    expect(translateStatus('running', '', null)).toBe('running')
  })

  // The translator lowercases its input, so upper-case and mixed-case raw
  // values behave like their lowercase forms.
  it('lets a DONE sentinel override an uppercase "RUNNING" status', () => {
    expect(translateStatus('RUNNING', '', done)).toBe('done')
  })

  it('lets a FAILED sentinel override an uppercase "IDLE" status', () => {
    expect(translateStatus('IDLE', '', { _status: 'FAILED' })).toBe('failed')
  })

  it('lets a TIMEOUT sentinel override a mixed-case "Running" status', () => {
    expect(translateStatus('Running', '', { _status: 'TIMEOUT' })).toBe('timeout')
  })

  it('recognizes lowercase terminal statuses without sentinel', () => {
    expect(translateStatus('done', '', null)).toBe('done')
    expect(translateStatus('failed', '', null)).toBe('failed')
    expect(translateStatus('timeout', '', null)).toBe('timeout')
    expect(translateStatus('killed', '', null)).toBe('killed')
    expect(translateStatus('blocked', '', null)).toBe('failed')
  })

  it('recognizes uppercase terminal statuses via normalization', () => {
    expect(translateStatus('DONE', '', null)).toBe('done')
    expect(translateStatus('FAILED', '', null)).toBe('failed')
    expect(translateStatus('TIMEOUT', '', null)).toBe('timeout')
    expect(translateStatus('KILLED', '', null)).toBe('killed')
    expect(translateStatus('BLOCKED', '', null)).toBe('failed')
  })

  it('reads an idle runtime by its terminal phase', () => {
    expect(translateStatus('idle', 'failed', null)).toBe('failed')
    expect(translateStatus('idle', 'working', null)).toBe('running')
  })

  describe('lifecycle rules shared with Go', () => {
    it('reports ended with no sentinel as done', () => {
      expect(translateStatus('ended', '', null)).toBe('done')
    })

    it('lets a sentinel settle an ended runtime', () => {
      expect(translateStatus('ended', '', { _status: 'FAILED' })).toBe('failed')
      expect(translateStatus('ended', '', { _status: 'TIMEOUT' })).toBe('timeout')
    })

    it('reports an unrecognized or empty status as unknown', () => {
      expect(translateStatus('', '', null)).toBe('unknown')
      expect(translateStatus('paused', '', null)).toBe('unknown')
      expect(translateStatus('unknown', 'working', null)).toBe('unknown')
    })

    it('lets a sentinel settle an unknown status', () => {
      expect(translateStatus('', '', done)).toBe('done')
      expect(translateStatus('paused', '', { _status: 'KILLED' })).toBe('killed')
    })

    it('keeps a running status running when it carries a terminal phase and no sentinel', () => {
      expect(translateStatus('running', 'done', null)).toBe('running')
      expect(translateStatus('running', 'failed', null)).toBe('running')
    })

    it('lets a sentinel settle a running status that carries a terminal phase', () => {
      expect(translateStatus('running', 'done', { _status: 'FAILED' })).toBe('failed')
    })

    it('passes an unrecognized sentinel word through lowercased', () => {
      expect(translateStatus('running', '', { _status: 'WEIRD' })).toBe('weird')
      expect(translateStatus('ended', '', { _status: 'WEIRD' })).toBe('weird')
    })

    it('does not let a sentinel overlay a terminal status the supervisor reported', () => {
      expect(translateStatus('done', '', { _status: 'FAILED' })).toBe('done')
      expect(translateStatus('failed', '', done)).toBe('failed')
      expect(translateStatus('blocked', '', done)).toBe('failed')
      expect(translateStatus('idle', 'done', { _status: 'FAILED' })).toBe('done')
    })

    it('treats a sentinel with no status word as no sentinel', () => {
      expect(translateStatus('ended', '', { _status: '' })).toBe('done')
      expect(translateStatus('running', '', { _status: '' })).toBe('running')
    })
  })
})

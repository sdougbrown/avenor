import * as path from 'node:path'

import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

import { describe, expect, test } from 'bun:test'

const CLI = path.join(import.meta.dir, 'cli.ts')

interface CliRun {
  exitCode: number
  stderr: string
}

function spawnCli(args: string[]): Promise<CliRun> {
  const proc = Bun.spawn({
    cmd: [process.execPath, CLI, ...args],
    stdout: 'ignore',
    stderr: 'pipe',
  })
  return new Response(proc.stderr).text().then(async (stderr) => ({
    exitCode: await proc.exited,
    stderr,
  }))
}

const requiredFlags = [
  '--socket',
  '/tmp/bridge.sock',
  '--workflow-id',
  'wf_1',
  '--webhook-url',
  'http://127.0.0.1:1/ask',
  '--decision-dir',
  '/tmp/decisions',
  '--template',
  '/tmp/template.json',
]

describe('cli argument validation', () => {
  test('exits 2 with usage text when required arguments are missing', async () => {
    const run = await spawnCli(['--socket', '/tmp/bridge.sock'])
    expect(run.exitCode).toBe(2)
    // Pinned to the full error line: flag names alone would be satisfied by
    // the usage banner, which lists every flag name.
    expect(run.stderr).toContain('Usage: avenor-escalation-bridge')
    expect(run.stderr).toContain(
      'error: missing required arguments: workflow-id, webhook-url, decision-dir, template',
    )
  })

  test('exits 2 with the parse error surfaced when a duration has no unit', async () => {
    const run = await spawnCli([...requiredFlags, '--wait', '5'])
    expect(run.exitCode).toBe(2)
    expect(run.stderr).toContain('Usage: avenor-escalation-bridge')
    expect(run.stderr).toContain('invalid duration "5"')
  })

  test('exits 2 with the parse error for a non-numeric duration', async () => {
    const run = await spawnCli([...requiredFlags, '--poll', 'lots'])
    expect(run.exitCode).toBe(2)
    expect(run.stderr).toContain('invalid duration "lots"')
  })

  test('exits 130 with the interrupted message on SIGINT', async () => {
    if (process.platform === 'win32') return // no SIGINT on Windows
    // Point the bridge at a nonexistent socket so it sits in its
    // reconnect/backoff loop; a real template keeps startup validation
    // from failing before the loop is reached.
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'escalation-bridge-cli-'))
    fs.writeFileSync(path.join(dir, 'template.json'), JSON.stringify({ nodes: [] }))
    const proc = Bun.spawn({
      cmd: [
        process.execPath,
        CLI,
        '--socket', path.join(dir, 'missing.sock'),
        '--workflow-id', 'wf_1',
        '--webhook-url', 'http://127.0.0.1:1/ask',
        '--decision-dir', path.join(dir, 'decisions'),
        '--template', path.join(dir, 'template.json'),
      ],
      stdout: 'pipe',
      stderr: 'ignore',
      stdin: 'ignore',
    })
    const decoder = new TextDecoder()
    let stdout = ''
    const reader = proc.stdout.getReader()
    try {
      // Interrupt once the bridge has entered its watch loop (it logs the
      // watching line before the first dial fails and backs off).
      while (!stdout.includes('bridge: watching')) {
        const { value, done } = await reader.read()
        if (done) break
        stdout += decoder.decode(value)
      }
      proc.kill('SIGINT')
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        stdout += decoder.decode(value)
      }
    } finally {
      reader.releaseLock()
    }
    expect(await proc.exited).toBe(130)
    expect(stdout).toContain('bridge: interrupted')
  }, 20_000)
})
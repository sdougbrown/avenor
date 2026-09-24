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
    expect(run.stderr).toContain('Usage: avenor-escalation-bridge')
    expect(run.stderr).toContain('missing required arguments:')
    expect(run.stderr).toContain('workflow-id')
    expect(run.stderr).toContain('webhook-url')
    expect(run.stderr).toContain('decision-dir')
    expect(run.stderr).toContain('template')
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
})
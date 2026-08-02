import { afterEach, beforeEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { ensureRunPaths } from '../paths.js'
import {
  externalRunMetadataPath,
  findExternalRun,
  forgetExternalRuns,
  listExternalRuns,
  registerExternalRun,
} from './run-registry.js'

const supervisorId = '/tmp/avenor-mcp-registry-test.sock'
let previousHome: string | undefined
let home = ''

function runInfo(runId: string, label: string, runtimeId: string) {
  const { sentinelPath, eventLogPath } = ensureRunPaths(runId)
  return {
    runId,
    label,
    runtimeId,
    supervisorId,
    sentinelPath,
    eventLogPath,
  }
}

describe('external run registry', () => {
  beforeEach(() => {
    previousHome = process.env.AVENOR_HOME
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-run-registry-'))
    process.env.AVENOR_HOME = home
    forgetExternalRuns(supervisorId)
  })

  afterEach(() => {
    forgetExternalRuns(supervisorId)
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
    fs.rmSync(home, { recursive: true, force: true })
  })

  it('rejects unsafe references at the metadata-path boundary', () => {
    expect(externalRunMetadataPath('safe_run-1')).toBe(
      path.join(home, 'runs', 'safe_run-1', 'run.json'),
    )

    for (const reference of ['', '.', '..', '../escape', 'nested/run', '/absolute', 'nested\\run']) {
      expect(() => externalRunMetadataPath(reference)).toThrow('unsafe external run reference')
    }
  })

  it('persists runtime identity and restores it after in-memory state is lost', () => {
    registerExternalRun({
      ...runInfo('public-run', 'demo', 'rt-1'),
      parentToken: 'secret-parent-token',
    })
    expect(fs.readFileSync(
      path.join(home, 'runs', 'public-run', 'run.json'),
      'utf-8',
    )).not.toContain('secret-parent-token')
    forgetExternalRuns(supervisorId)

    expect(findExternalRun(supervisorId, 'public-run')).toMatchObject({
      runId: 'public-run',
      label: 'demo',
      runtimeId: 'rt-1',
    })
    forgetExternalRuns(supervisorId)
    expect(findExternalRun(supervisorId, 'demo')?.runtimeId).toBe('rt-1')
    expect(findExternalRun('/tmp/other-supervisor.sock', 'public-run')).toBeUndefined()
    expect(fs.statSync(path.join(home, 'runs', 'public-run', 'run.json')).mode & 0o077).toBe(0)
  })

  it('never lets a label alias replace an exact public run ID', () => {
    registerExternalRun(runInfo('victim-run', 'victim', 'rt-victim'))
    registerExternalRun(runInfo('attacker-run', 'victim-run', 'rt-attacker'))

    expect(findExternalRun(supervisorId, 'victim-run')?.runtimeId).toBe('rt-victim')
    expect(listExternalRuns(supervisorId).map(run => run.runId).sort()).toEqual([
      'attacker-run',
      'victim-run',
    ])
  })
})

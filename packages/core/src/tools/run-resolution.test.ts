import { describe, expect, it } from 'bun:test'
import type { RunInfo, Supervisor } from '../supervisor.js'
import { findLocalRunByReference } from './run-resolution.js'

function run(runId: string, label: string): RunInfo {
  return {
    runId,
    label,
    sentinelPath: `/runs/${runId}/sentinel.done`,
    eventLogPath: `/runs/${runId}/events.log`,
  }
}

function localSupervisor(entries: Array<[string, RunInfo]>): Supervisor {
  return { runs: new Map(entries) } as unknown as Supervisor
}

describe('local run resolution', () => {
  it('prefers an exact map key over a conflicting label alias', () => {
    const exact = run('public-run', 'friendly')
    const alias = run('other-run', 'public-run')
    const sup = localSupervisor([
      [exact.runId, exact],
      [alias.runId, alias],
    ])

    expect(findLocalRunByReference(sup, 'public-run')).toBe(exact)
  })

  it('falls back to a label only when no exact map key exists', () => {
    const labeled = run('public-run', 'friendly')
    const sup = localSupervisor([[labeled.runId, labeled]])

    expect(findLocalRunByReference(sup, 'friendly')).toBe(labeled)
  })

  it('returns no run when neither a map key nor label matches', () => {
    const sup = localSupervisor([['public-run', run('public-run', 'friendly')]])

    expect(findLocalRunByReference(sup, 'missing')).toBeUndefined()
  })
})

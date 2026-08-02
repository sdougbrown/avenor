import { Supervisor, type RunInfo } from '../supervisor.js'

type SupervisorRuns = {
  runs: Map<string, RunInfo>
}

/**
 * Resolve a local supervisor reference. An exact map key deliberately wins
 * over a label alias so aliases cannot shadow public run IDs.
 */
export function findLocalRunByReference(
  sup: Supervisor,
  reference: string,
): RunInfo | undefined {
  const runs = (sup as unknown as SupervisorRuns).runs
  const byKey = runs.get(reference)
  if (byKey) return byKey
  for (const info of runs.values()) {
    if (info.label === reference) return info
  }
  return undefined
}

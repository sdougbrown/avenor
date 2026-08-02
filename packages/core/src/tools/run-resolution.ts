import { Supervisor, type RunInfo } from '../supervisor.js'

type SupervisorRuns = {
  runs: Map<string, RunInfo>
  aliases: Map<string, RunInfo>
}

/** Resolve a local public run ID before considering a label alias. */
export function findLocalRunByReference(
  sup: Supervisor,
  reference: string,
): RunInfo | undefined {
  const { runs, aliases } = sup as unknown as SupervisorRuns
  return runs.get(reference) ?? aliases.get(reference)
}

/** Match a supervisor list entry to its stored runtime identity. */
export function findLocalRunByRuntimeId(
  sup: Supervisor,
  runtimeId: string,
): RunInfo | undefined {
  if (!runtimeId) return undefined
  const { runs } = sup as unknown as SupervisorRuns
  for (const runInfo of runs.values()) {
    if (runInfo.runtimeId === runtimeId) return runInfo
  }
  return undefined
}

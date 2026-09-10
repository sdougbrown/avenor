import { Supervisor, type RunInfo } from '../supervisor.js'

/** Resolve a local public run ID before considering a label alias. */
export function findLocalRunByReference(
  sup: Supervisor,
  reference: string,
): RunInfo | undefined {
  return sup.getRunByReference(reference)
}

/** Match a supervisor list entry to its stored runtime identity. */
export function findLocalRunByRuntimeId(
  sup: Supervisor,
  runtimeId: string,
): RunInfo | undefined {
  return sup.getRunByRuntimeId(runtimeId)
}

import type { RunInfo } from '../supervisor.js'
import type { Client } from '../client.js'
import { validateRunId } from './validate.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'
import { findExternalRun } from './run-registry.js'
import { findLocalRunByReference } from './run-resolution.js'

export interface KillToolArgs {
  runId: string
  supervisorId?: string
}

export interface KillResult {
  killed: boolean
  run_id: string
  runtime_id?: string
}

// A bare runtime ID also matches: the control server keys runs by runtime
// ID, and the list entries carry it alongside the public run ID and label.
function matchEntry(entry: Record<string, unknown>, reference: string): string | undefined {
  const runtimeId = typeof entry.runtime_id === 'string' ? entry.runtime_id : ''
  if (!runtimeId) return undefined
  if (runtimeId === reference) return runtimeId
  if (typeof entry.label === 'string' && entry.label === reference) return runtimeId
  if (typeof entry.run_id === 'string' && entry.run_id === reference) return runtimeId
  if (typeof entry.session_id === 'string' && entry.session_id === reference) return runtimeId
  return undefined
}

async function resolveRuntimeId(client: Client, reference: string): Promise<string> {
  const list = await client.list()
  for (const entry of list) {
    const runtimeId = matchEntry(entry, reference)
    if (runtimeId) return runtimeId
  }
  throw new Error(`run not found: ${reference}`)
}

async function executeKillTool(
  args: KillToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<KillResult> {
  // Validate before any supervisor interaction: an empty or malformed
  // reference must never reach the control server, where cancel without a
  // runtime_id targets the supervisor's own root run.
  validateRunId(args.runId)

  const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
  try {
    const runInfo: RunInfo | undefined = isSingleton && sup
      ? findLocalRunByReference(sup, args.runId)
      : findExternalRun(supervisorId, args.runId)
    const runtimeId = runInfo?.runtimeId ?? await resolveRuntimeId(client, args.runId)
    await client.cancel(runtimeId)
    return {
      killed: true,
      run_id: args.runId,
      runtime_id: runtimeId,
    }
  } finally {
    if (!isSingleton) {
      client.close()
    }
  }
}

export function createKillTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: KillToolArgs) => Promise<KillResult> {
  return args => executeKillTool(args, getSupervisorClient)
}

export const killTool = createKillTool(realGetSupervisorClient)

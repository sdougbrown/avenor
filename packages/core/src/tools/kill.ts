import { Supervisor } from '../supervisor.js'
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

async function executeKillTool(
  args: KillToolArgs,
  getSupervisorClient: typeof realGetSupervisorClient,
): Promise<KillResult> {
  if (args.supervisorId) {
    const { client, isSingleton, sup, supervisorId } = await getSupervisorClient(args.supervisorId)
    try {
      validateRunId(args.runId)
      const runInfo = isSingleton && sup
        ? findLocalRunByReference(sup, args.runId)
        : findExternalRun(supervisorId, args.runId)
      const runtimeId = runInfo?.runtimeId ?? args.runId
      await client.cancel(runtimeId)
      return {
        killed: true,
        run_id: args.runId,
        ...(runInfo?.runtimeId && { runtime_id: runInfo.runtimeId }),
      }
    } finally {
      if (!isSingleton) {
        client.close()
      }
    }
  }

  const sup = await Supervisor.get()
  const runInfo = findLocalRunByReference(sup, args.runId)
  const runtimeId = runInfo?.runtimeId ?? args.runId
  await sup.getClient().cancel(runtimeId)
  return {
    killed: true,
    run_id: args.runId,
    ...(runInfo?.runtimeId && { runtime_id: runInfo.runtimeId }),
  }
}

export function createKillTool(
  getSupervisorClient: typeof realGetSupervisorClient,
): (args: KillToolArgs) => Promise<KillResult> {
  return args => executeKillTool(args, getSupervisorClient)
}

export const killTool = createKillTool(realGetSupervisorClient)

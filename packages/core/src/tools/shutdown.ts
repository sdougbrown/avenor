import * as fs from 'node:fs'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { dial } from '../client.js'
import { validateSupervisorSocketPath } from './validate.js'
import { clearExternalRuns, externalRunMetadataPath } from './run-registry.js'

export async function shutdownTool(args: {
  supervisorId?: string
  force?: boolean
}): Promise<{ ok: boolean; cleaned_up: string[] }> {
  const cleanedUp: string[] = []

  if (args.supervisorId) {
    const supervisorId = validateSupervisorSocketPath(args.supervisorId)
    const isSingleton = Supervisor.isCurrentInstance(supervisorId)

    if (isSingleton) {
      const sup = Supervisor.currentInstance()
      if (!sup) {
        throw new Error('supervisor not started')
      }
      const client = sup.getClient()
      await client.shutdown(args.force ? 'force' : 'graceful')

      const runs = (sup as any).runs as Map<string, RunInfo>
      for (const info of runs.values()) {
        try {
          await fs.promises.unlink(info.sentinelPath)
          cleanedUp.push(info.sentinelPath)
        } catch {}
        try {
          await fs.promises.unlink(info.eventLogPath)
          cleanedUp.push(info.eventLogPath)
        } catch {}
      }

      await sup.close({ skipShutdown: true })
    } else {
      const client = await dial(supervisorId)
      try {
        // A failed shutdown may leave live runtimes. Keep their durable mappings
        // instead of orphaning them by cleaning artifacts on an unconfirmed stop.
        await client.shutdown(args.force ? 'force' : 'graceful')
      } finally {
        client.close()
      }

      for (const info of clearExternalRuns(supervisorId)) {
        try {
          await fs.promises.unlink(info.sentinelPath)
          cleanedUp.push(info.sentinelPath)
        } catch {}
        try {
          await fs.promises.unlink(info.eventLogPath)
          cleanedUp.push(info.eventLogPath)
        } catch {}
        const metadataPath = externalRunMetadataPath(info.runId)
        try {
          await fs.promises.unlink(metadataPath)
          cleanedUp.push(metadataPath)
        } catch {}
      }
    }

    return { ok: true, cleaned_up: cleanedUp }
  }

  const sup = await Supervisor.get()

  try {
    const client = sup.getClient()
    await client.shutdown(args.force ? 'force' : 'graceful')
  } catch {
    // shutdown may fail if already shutting down
  }

  const runs = (sup as any).runs as Map<string, RunInfo>
  for (const info of runs.values()) {
    try {
      fs.unlinkSync(info.sentinelPath)
      cleanedUp.push(info.sentinelPath)
    } catch {}
    try {
      fs.unlinkSync(info.eventLogPath)
      cleanedUp.push(info.eventLogPath)
    } catch {}
  }

  await sup.close({ skipShutdown: true })

  return { ok: true, cleaned_up: cleanedUp }
}

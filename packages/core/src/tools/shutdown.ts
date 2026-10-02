import * as fs from 'node:fs'
import { Supervisor } from '../supervisor.js'
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
    const sup = Supervisor.getInstance(supervisorId)

    if (sup) {
      const client = sup.getClient()
      await client.shutdown(args.force ? 'force' : 'graceful')

      const cleanups: Promise<void>[] = []
      sup.forEachRun(info => {
        cleanups.push((async () => {
          try {
            await fs.promises.unlink(info.sentinelPath)
            cleanedUp.push(info.sentinelPath)
          } catch {}
          try {
            await fs.promises.unlink(info.eventLogPath)
            cleanedUp.push(info.eventLogPath)
          } catch {}
        })())
      })
      await Promise.all(cleanups)

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

  sup.forEachRun(info => {
    try {
      fs.unlinkSync(info.sentinelPath)
      cleanedUp.push(info.sentinelPath)
    } catch {}
    try {
      fs.unlinkSync(info.eventLogPath)
      cleanedUp.push(info.eventLogPath)
    } catch {}
  })

  await sup.close({ skipShutdown: true })

  return { ok: true, cleaned_up: cleanedUp }
}

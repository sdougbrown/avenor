import * as fs from 'node:fs'
import { Supervisor, type RunInfo } from '../supervisor.js'
import { dial } from '../client.js'
import { validateSupervisorSocketPath } from './validate.js'

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

      await sup.close()
    } else {
      const client = await dial(supervisorId)
      try {
        await client.shutdown(args.force ? 'force' : 'graceful')
      } catch {
        // ignore
      } finally {
        client.close()
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

  await sup.close()

  return { ok: true, cleaned_up: cleanedUp }
}

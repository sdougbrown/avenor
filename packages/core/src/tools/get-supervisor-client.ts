import { Supervisor } from '../supervisor.js'
import { dial, type Client } from '../client.js'
import { validateSupervisorSocketPath } from './validate.js'

export async function getSupervisorClient(supervisorId: string | undefined): Promise<{
  client: Client
  isSingleton: boolean
  sup: Supervisor | null
  supervisorId: string
}> {
  if (supervisorId === undefined) {
    // No explicit supervisor: resolve the in-process singleton, starting one
    // if necessary (same fallback result.ts and spawn.ts rely on).
    const sup = await Supervisor.get()
    return { client: sup.getClient(), isSingleton: true, sup, supervisorId: sup.supervisorId }
  }
  const id = validateSupervisorSocketPath(supervisorId)
  const current = Supervisor.currentInstance()
  const isSingleton = current !== null &&
    validateSupervisorSocketPath(current.supervisorId) === id
  if (isSingleton) {
    return { client: current.getClient(), isSingleton: true, sup: current, supervisorId: id }
  }
  return { client: await dial(id), isSingleton: false, sup: null, supervisorId: id }
}

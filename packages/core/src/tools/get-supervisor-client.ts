import { Supervisor } from '../supervisor.js'
import { dial, type Client } from '../client.js'
import { attachOwner } from '../owner-attach.js'
import { validateSupervisorSocketPath } from '../paths.js'

export interface SupervisorClientOptions {
  /** Attach with the socket's published owner capability instead of a bare dial. */
  owner?: boolean
}

export async function getSupervisorClient(
  supervisorId: string | undefined,
  options: SupervisorClientOptions = {},
): Promise<{
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
  const current = Supervisor.getInstance(id)
  const isSingleton = current !== null
  if (isSingleton) {
    return { client: current.getClient(), isSingleton: true, sup: current, supervisorId: id }
  }
  return {
    client: options.owner ? await attachOwner(id) : await dial(id),
    isSingleton: false,
    sup: null,
    supervisorId: id,
  }
}

import { Supervisor } from '../supervisor.js'
import { dial, type Client } from '../client.js'
import { validateSupervisorSocketPath } from './validate.js'

export async function getSupervisorClient(supervisorId: string): Promise<{
  client: Client
  isSingleton: boolean
  sup: Supervisor | null
  supervisorId: string
}> {
  const id = validateSupervisorSocketPath(supervisorId)
  const isSingleton = Supervisor.isCurrentInstance(id)
  if (isSingleton) {
    const sup = Supervisor.currentInstance()
    if (!sup) throw new Error(`Supervisor.isCurrentInstance() returned true but currentInstance() is null (id: ${id})`)
    return { client: sup.getClient(), isSingleton: true, sup, supervisorId: id }
  }
  return { client: await dial(id), isSingleton: false, sup: null, supervisorId: id }
}

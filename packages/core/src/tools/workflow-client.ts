import { Supervisor } from '../supervisor.js'
import type { Client } from '../client.js'
import { getSupervisorClient as realGetSupervisorClient } from './get-supervisor-client.js'

export type GetSupervisorClient = typeof realGetSupervisorClient

/**
 * Run `work` against the client for the given supervisor, or the singleton when none is
 * supplied. External supervisors are dialed on demand and closed again; the singleton client is
 * reused. Workflow host tools correlate by supervisor ID so runtime ids (e.g. rt_1) that collide
 * across supervisors never route to the wrong host.
 */
export async function withWorkflowClient<T>(
  supervisorId: string | undefined,
  work: (client: Client) => Promise<T>,
  getSupervisorClient: GetSupervisorClient = realGetSupervisorClient,
): Promise<T> {
  if (supervisorId) {
    const { client, isSingleton } = await getSupervisorClient(supervisorId)
    try {
      return await work(client)
    } finally {
      if (!isSingleton) client.close()
    }
  }
  const sup = await Supervisor.get()
  return work(sup.getClient())
}

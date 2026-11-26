import {
  createHostMessageProvider,
  getSupervisorClient,
  installAskClaims,
  messageTool,
  peersTool,
  SUPERVISOR_RUN_ID,
  type PendingAsk,
} from '@dougbots/avenor-core'

type HostOptions = Parameters<typeof createHostMessageProvider>[0]
/** The control client the supervisor hands out; a superset of MessageClient. */
export type MessagingClient = Awaited<ReturnType<typeof getSupervisorClient>>['client']
type PendingAskMap = Map<string, PendingAsk>

/** The broker identity a host surface answers as. */
export const HOST_RUN_ID = SUPERVISOR_RUN_ID

export interface MessagingHost {
  /** The run id children address when they message the harness back. */
  readonly hostRunId: string
  message(args: unknown): Promise<unknown>
  peers(args: unknown): Promise<unknown>
}

export interface MessagingHostOptions {
  readonly client: MessagingClient
  readonly supervisorId: string
  readonly surface?: string
}

/**
 * Bind Avenor's message router to this harness's supervisor.
 *
 * The router is core's, so the six actions and their failure text exist once.
 * What is harness-specific is the surface: a host provider reads the peer
 * listing and the inbound ask map through the control client, and answers as
 * the `supervisor` run.
 */
export async function createMessagingHost(options: MessagingHostOptions): Promise<MessagingHost> {
  const { generation } = await options.client.messagePreflight()
  const pendingAsks = new Map() as PendingAskMap
  const claims = new Map<string, { asker: string; generation: number }>()
  const pollGeneration = 0

  const provider = createHostMessageProvider({
    surface: (options.surface ?? 'dsh-host') as HostOptions['surface'],
    supervisorId: options.supervisorId,
    generation,
    client: options.client,
    pendingAsks,
    pollGeneration: () => pollGeneration,
  })
  // No inbound poll runs yet, so every ask claim is scoped to generation zero
  // and expires with the process. Wiring the receive loop is what fills
  // `pendingAsks`; the claim pair exists so a reply can release through it.
  installAskClaims(provider, claims, pendingAsks, () => pollGeneration)

  return {
    hostRunId: SUPERVISOR_RUN_ID,
    message: (args) => messageTool(args, provider),
    peers: (args) => peersTool(args, provider),
  }
}

/**
 * A broker-backed host built on first use. Activation never starts a
 * supervisor: a profile that installs the bundle but never messages pays
 * nothing.
 */
export function createLazyMessagingHost(): () => Promise<MessagingHost> {
  let pending: Promise<MessagingHost> | undefined
  return () => {
    pending ??= (async () => {
      const { client, supervisorId } = await getSupervisorClient(undefined)
      return createMessagingHost({ client, supervisorId })
    })()
    return pending
  }
}

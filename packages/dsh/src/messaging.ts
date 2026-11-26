import {
  brokerReceiveTool,
  createHostMessageProvider,
  getSupervisorClient,
  installAskClaims,
  messageTool,
  peersTool,
  SUPERVISOR_RUN_ID,
  type BrokerReceiveResult,
  type InboundAsk,
  type PendingAsk,
} from '@dougbots/avenor-core'
import { inboundMessage } from './inbound.js'
import type { ParentAgent } from './types.js'

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
  /** The runtime's inbound ask map, so a delivered ask and its reply agree. */
  readonly pendingAsks?: PendingAskMap
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
  const pendingAsks = options.pendingAsks ?? (new Map() as PendingAskMap)
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
  // A reply releases its claim through this pair; the receive loop fills
  // `pendingAsks`, so an ask that was never delivered has no claim to release.
  installAskClaims(provider, claims, pendingAsks, () => pollGeneration)

  return {
    hostRunId: SUPERVISOR_RUN_ID,
    message: (args) => messageTool(args, provider),
    peers: (args) => peersTool(args, provider),
  }
}

/** Receives one batch of inbound frames; injectable so the loop is testable. */
export type InboundReceive = (args: {
  generation?: string
  timeout_ms?: number
}) => Promise<BrokerReceiveResult>

export interface MessagingRuntimeOptions {
  /** Overrides the lazily built broker-backed host. */
  readonly host?: () => Promise<MessagingHost>
  readonly receive?: InboundReceive
  /** Bound on one receive wait; a short one keeps stop() responsive. */
  readonly pollTimeoutMs?: number
  readonly onError?: (error: unknown) => void
}

/**
 * Owns the pieces that outlive one message: the lazily built host, the mapping
 * from a delegated run to the agent that started it, and the inbound poll loop.
 *
 * Nothing here touches the broker until it is needed — a message tool call, a
 * delegation, or both.
 */
export interface MessagingRuntime {
  host(): Promise<MessagingHost>
  /** Remember which agent started a run, so an inbound frame can reach it. */
  remember(runId: string, parent: ParentAgent): void
  forget(runId: string): void
  parentFor(runId: string): ParentAgent | undefined
  /** Register an ask whose reply must reach the sender. */
  registerAsk(ask: InboundAsk): void
  /** Start the receive loop once; repeated calls are no-ops. */
  ensureInbound(): void
  stop(): void
}

export function createMessagingRuntime(options: MessagingRuntimeOptions = {}): MessagingRuntime {
  const parents = new Map<string, ParentAgent>()
  const pendingAsks = new Map() as PendingAskMap
  const receive: InboundReceive = options.receive ?? ((args) => brokerReceiveTool(args))
  const pollTimeoutMs = options.pollTimeoutMs ?? 1000
  let hostPromise: Promise<MessagingHost> | undefined
  let inbound: { stop(): void } | undefined

  const runtime: MessagingRuntime = {
    host() {
      hostPromise ??= (options.host ?? defaultHost(pendingAsks))()
      return hostPromise
    },
    remember(runId, parent) {
      parents.set(runId, parent)
    },
    forget(runId) {
      parents.delete(runId)
    },
    parentFor(runId) {
      return parents.get(runId)
    },
    registerAsk(ask) {
      pendingAsks.set(ask.message_id, {
        from_run_id: ask.from_run_id,
        message_id: ask.message_id,
      })
    },
    ensureInbound() {
      inbound ??= startInboundLoop({
        receive,
        pollTimeoutMs,
        onAsk: (ask) => deliverInbound(runtime, ask),
        onError: options.onError,
      })
    },
    stop() {
      inbound?.stop()
      inbound = undefined
    },
  }
  return runtime
}

function defaultHost(pendingAsks: PendingAskMap): () => Promise<MessagingHost> {
  return async () => {
    const { client, supervisorId } = await getSupervisorClient(undefined)
    return createMessagingHost({ client, supervisorId, pendingAsks })
  }
}

/**
 * Deliver one inbound frame to the agent that started its sender, and register
 * an ask so `avenor_message action=reply` can reach it. A frame from a run this
 * harness did not start is dropped: there is no agent to wake, and delivering
 * it to an arbitrary one would let any broker peer write into the session.
 */
export function deliverInbound(runtime: MessagingRuntime, ask: InboundAsk): boolean {
  const parent = runtime.parentFor(ask.from_run_id)
  if (parent === undefined) return false

  const message = inboundMessage(ask)
  if (parent.status === 'running') parent.inject(message)
  else parent.followup(message)

  if (ask.expects_reply) runtime.registerAsk(ask)
  return true
}

export interface InboundLoopOptions {
  readonly receive: InboundReceive
  readonly pollTimeoutMs: number
  readonly onAsk: (ask: InboundAsk) => void
  readonly onError?: (error: unknown) => void
}

/**
 * Poll the broker until stopped. A failed poll reports and continues: a broker
 * that is briefly unreachable must not silently end message delivery.
 */
export function startInboundLoop(options: InboundLoopOptions): { stop(): void } {
  let stopped = false
  let generation: string | undefined

  const run = async (): Promise<void> => {
    while (!stopped) {
      try {
        const result = await options.receive({ generation, timeout_ms: options.pollTimeoutMs })
        generation = result.generation ?? generation
        for (const ask of result.asks) {
          if (stopped) return
          options.onAsk(ask)
        }
      } catch (error) {
        options.onError?.(error)
        await new Promise((resolve) => setTimeout(resolve, options.pollTimeoutMs))
      }
    }
  }

  void run()
  return {
    stop() {
      stopped = true
    },
  }
}

import {
  createHostMessageProvider,
  getSupervisorClient,
  installAskClaims,
  messageTool,
  peersTool,
  postBroker,
  SUPERVISOR_RUN_ID,
  type PendingAsk,
} from '@dougbots/avenor-core'
import { inboundFrameToAsk, inboundMessage } from './inbound.js'
import type { ParentAgent } from './types.js'

type HostOptions = Parameters<typeof createHostMessageProvider>[0]
/** The control client the supervisor hands out; a superset of MessageClient. */
export type MessagingClient = Awaited<ReturnType<typeof getSupervisorClient>>['client']
type PendingAskMap = Map<string, PendingAsk>

/** The broker identity a host surface answers as when it is not bound. */
export const HOST_RUN_ID = SUPERVISOR_RUN_ID

export interface MessagingHost {
  readonly hostRunId: string
  message(args: unknown): Promise<unknown>
  peers(args: unknown): Promise<unknown>
}

/**
 * One native conversation's inbox. A DeepSeek Harness session binds as host
 * `dsh`, so each session mints its own run and its children address that run
 * rather than sharing the supervisor's single inbox.
 */
export interface BoundConversation {
  readonly sessionId: string
  readonly runId: string
  readonly brokerUrl: string
  readonly token: string
}

/** One frame from the broker's control feed; the fields this plugin reads. */
export interface BrokerFrame {
  readonly id?: string
  readonly from_run_id?: string
  readonly sender?: string
  readonly payload?: {
    readonly id?: string
    readonly message?: string
    readonly expects_reply?: boolean
    readonly from_run_id?: string
  }
}

export interface MessagingHostOptions {
  readonly client: MessagingClient
  readonly supervisorId: string
  readonly surface?: string
  readonly pendingAsks?: PendingAskMap
}

/**
 * Bind Avenor's message router to this harness's supervisor.
 *
 * The router is core's, so the six actions and their failure text exist once.
 * What is harness-specific is the surface: a host provider reads the peer
 * listing and the inbound ask map through the control client.
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
  installAskClaims(provider, claims, pendingAsks, () => pollGeneration)

  return {
    hostRunId: SUPERVISOR_RUN_ID,
    message: (args) => messageTool(args, provider),
    peers: (args) => peersTool(args, provider),
  }
}

export interface MessagingRuntimeOptions {
  readonly host?: () => Promise<MessagingHost>
  /** Binds one session's conversation; defaults to the control client's message_bind. */
  readonly bind?: (sessionId: string) => Promise<BoundConversation>
  /** Reads one bound conversation's feed; defaults to the broker's /poll-control. */
  readonly poll?: (bound: BoundConversation) => Promise<BrokerFrame[]>
  readonly pollTimeoutMs?: number
  readonly onError?: (error: unknown) => void
}

export interface MessagingRuntime {
  host(): Promise<MessagingHost>
  /** Bind (or reuse) a session's conversation; the run id is the spawn parent. */
  bind(sessionId: string): Promise<BoundConversation>
  remember(runId: string, parent: ParentAgent): void
  forget(runId: string): void
  parentFor(runId: string): ParentAgent | undefined
  registerAsk(messageId: string, fromRunId: string): void
  ensureInbound(sessionId: string): void
  stop(): void
}

export function createMessagingRuntime(options: MessagingRuntimeOptions = {}): MessagingRuntime {
  const parents = new Map<string, ParentAgent>()
  const pendingAsks = new Map() as PendingAskMap
  const bindings = new Map<string, Promise<BoundConversation>>()
  const loops = new Map<string, { stop(): void }>()
  const poll = options.poll ?? ((bound) => pollControl(bound))
  const pollIntervalMs = options.pollTimeoutMs ?? 1000
  let hostPromise: Promise<MessagingHost> | undefined
  let clientPromise: Promise<{ client: MessagingClient; supervisorId: string }> | undefined

  const control = () => {
    clientPromise ??= getSupervisorClient(undefined)
    return clientPromise
  }

  const runtime: MessagingRuntime = {
    host() {
      hostPromise ??= options.host ? options.host() : defaultHost(pendingAsks, control)
      return hostPromise
    },
    bind(sessionId) {
      let pending = bindings.get(sessionId)
      if (!pending) {
        const binder = options.bind ?? (async (id: string) => bindSession(await control(), id))
        pending = binder(sessionId)
        bindings.set(sessionId, pending)
      }
      return pending
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
    registerAsk(messageId, fromRunId) {
      pendingAsks.set(messageId, { from_run_id: fromRunId, message_id: messageId })
    },
    ensureInbound(sessionId) {
      if (loops.has(sessionId)) return
      loops.set(
        sessionId,
        startInboundLoop({
          sessionId,
          runtime,
          poll,
          pollIntervalMs,
          onError: options.onError,
        }),
      )
    },
    stop() {
      for (const loop of loops.values()) loop.stop()
      loops.clear()
    },
  }
  return runtime
}

async function defaultHost(
  pendingAsks: PendingAskMap,
  control: () => Promise<{ client: MessagingClient; supervisorId: string }>,
): Promise<MessagingHost> {
  const { client, supervisorId } = await control()
  return createMessagingHost({ client, supervisorId, pendingAsks })
}

async function bindSession(
  control: { client: MessagingClient },
  sessionId: string,
): Promise<BoundConversation> {
  const bound = await control.client.messageBind({ host: 'dsh', session_id: sessionId })
  return {
    sessionId,
    runId: bound.run_id,
    brokerUrl: bound.broker_url,
    token: bound.run_token,
  }
}

/** Read one conversation's feed: the queue its bound run consumes. */
export async function pollControl(bound: BoundConversation): Promise<BrokerFrame[]> {
  const frames = await postBroker(
    { brokerUrl: bound.brokerUrl, runId: bound.runId, token: bound.token },
    '/poll-control',
    {},
    fetch,
    AbortSignal.timeout(10_000),
  )
  return Array.isArray(frames) ? (frames as BrokerFrame[]) : []
}

/**
 * Deliver one inbound frame to the agent that started its sender.
 *
 * The broker reports the sender as a runtime id (`rt_N`) in `from_run_id` and
 * the public run id in `sender`; both are registered, because which one names a
 * delegation depends on where the spawn was made.
 */
export function deliverFrame(runtime: MessagingRuntime, frame: BrokerFrame): boolean {
  const sender = frame.payload?.from_run_id ?? frame.from_run_id ?? ''
  const parent = runtime.parentFor(sender) ?? runtime.parentFor(frame.sender ?? '')
  if (parent === undefined) return false

  const ask = inboundFrameToAsk(frame)
  if (ask === undefined) return false

  const message = inboundMessage(ask)
  if (parent.status === 'running') parent.inject(message)
  else parent.followup(message)

  if (ask.expectsReply) runtime.registerAsk(ask.messageId, ask.fromRunId)
  return true
}

export interface InboundLoopOptions {
  readonly sessionId: string
  readonly runtime: MessagingRuntime
  readonly poll: (bound: BoundConversation) => Promise<BrokerFrame[]>
  readonly pollIntervalMs: number
  readonly onError?: (error: unknown) => void
}

/**
 * Poll one conversation's feed until stopped. A failed poll reports and
 * continues: a broker that is briefly unreachable must not end delivery.
 */
export function startInboundLoop(options: InboundLoopOptions): { stop(): void } {
  let stopped = false

  const run = async (): Promise<void> => {
    let bound: BoundConversation
    try {
      bound = await options.runtime.bind(options.sessionId)
    } catch (error) {
      options.onError?.(error)
      return
    }
    while (!stopped) {
      try {
        for (const frame of await options.poll(bound)) {
          if (stopped) return
          deliverFrame(options.runtime, frame)
        }
      } catch (error) {
        options.onError?.(error)
      }
      // One pause per turn: a poll that returns empty must yield the event
      // loop, or delivery would spin and starve every timer in the host.
      await new Promise((resolve) => setTimeout(resolve, options.pollIntervalMs))
    }
  }

  void run()
  return {
    stop() {
      stopped = true
    },
  }
}

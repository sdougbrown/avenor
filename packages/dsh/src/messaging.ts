import { hostname as osHostname } from 'node:os'
import {
  createBrokerChildBroker,
  createChildMessageProvider,
  createHostMessageProvider,
  defaultHostLabel,
  getSupervisorClient,
  gitToplevel,
  installAskClaims,
  messageTool,
  peersTool,
  postBroker,
  publishHarnessName,
  SUPERVISOR_RUN_ID,
  type MessageProvider,
  type PendingAsk,
} from '@dougbots/avenor-core'
import { inboundFrameToAsk, inboundMessage } from './inbound.js'
import type { ParentAgent } from './types.js'

type HostOptions = Parameters<typeof createHostMessageProvider>[0]
/** The control client the supervisor hands out; a superset of MessageClient. */
export type MessagingClient = Awaited<ReturnType<typeof getSupervisorClient>>['client']
type PendingAskMap = Map<string, PendingAsk>

/** The broker identity a host surface answers as when it has no binding. */
export const HOST_RUN_ID = SUPERVISOR_RUN_ID

/** The two calls the message tools make, routed by the calling session. */
export interface MessagingSurface {
  message(args: unknown, sessionId?: string): Promise<unknown>
  peers(args: unknown, sessionId?: string): Promise<unknown>
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

export interface BindSeed {
  /** The session's working directory, used to name its conversation. */
  readonly dir?: string
}

export interface MessagingRuntimeOptions {
  /** Binds one session's conversation; defaults to the control client's message_bind. */
  readonly bind?: (sessionId: string, seed?: BindSeed) => Promise<BoundConversation>
  /**
   * Publishes the bound run's display name; defaults to the broker's
   * `/session_name`. Injectable so a test can observe the label without a
   * broker.
   */
  readonly publishName?: (bound: BoundConversation, dir: string) => Promise<string | undefined>
  /** Reads one bound conversation's feed; defaults to the broker's /poll-control. */
  readonly poll?: (bound: BoundConversation) => Promise<BrokerFrame[]>
  /** The provider a call with no usable session falls back to. */
  readonly hostProvider?: () => Promise<MessageProvider>
  /** Builds the provider a bound session speaks through; injectable for tests. */
  readonly boundProvider?: (bound: BoundConversation) => MessageProvider
  /** The calling agent's working directory, used to name a conversation. */
  readonly dirFor?: (parent: ParentAgent) => string | undefined
  readonly pollIntervalMs?: number
  readonly onError?: (error: unknown) => void
}

export interface MessagingRuntime extends MessagingSurface {
  /** Bind (or reuse) a session's conversation; the run id is the spawn parent. */
  bind(sessionId: string, seed?: BindSeed): Promise<BoundConversation>
  dirFor(parent: ParentAgent): string | undefined
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
  const boundProviders = new Map<string, MessageProvider>()
  const loops = new Map<string, { stop(): void }>()
  const poll = options.poll ?? pollControl
  const pollIntervalMs = options.pollIntervalMs ?? 1000
  let controlPromise: Promise<{ client: MessagingClient; supervisorId: string }> | undefined
  let hostProviderPromise: Promise<MessageProvider> | undefined

  const control = () => {
    controlPromise ??= getSupervisorClient(undefined)
    return controlPromise
  }

  /**
   * A call speaks as the caller's own conversation run. A reply's ask edge
   * lives on that run, so answering from the supervisor's identity is refused
   * as edge-absent; the child provider talks to the broker with the bound
   * credentials instead.
   */
  async function providerFor(sessionId: string | undefined): Promise<MessageProvider> {
    if (sessionId === undefined) {
      hostProviderPromise ??= options.hostProvider
        ? options.hostProvider()
        : defaultHostProvider(pendingAsks, control)
      return hostProviderPromise
    }
    const bound = await runtime.bind(sessionId)
    let provider = boundProviders.get(sessionId)
    if (provider === undefined) {
      if (options.boundProvider) {
        provider = options.boundProvider(bound)
        boundProviders.set(sessionId, provider)
        return provider
      }
      provider = createChildMessageProvider({
        surface: 'dsh' as HostOptions['surface'],
        runId: bound.runId,
        broker: createBrokerChildBroker({
          brokerUrl: bound.brokerUrl,
          runId: bound.runId,
          token: bound.token,
        }),
        pendingAsks,
        pollGeneration: () => 0,
      })
      boundProviders.set(sessionId, provider)
    }
    return provider
  }

  const runtime: MessagingRuntime = {
    async message(args, sessionId) {
      return messageTool(args, await providerFor(sessionId))
    },
    async peers(args, sessionId) {
      return peersTool(args, await providerFor(sessionId))
    },
    bind(sessionId, seed) {
      let pending = bindings.get(sessionId)
      if (!pending) {
        const binder = options.bind ?? (async (id: string) => bindSession(await control(), id))
        const publish = options.publishName ?? publishBoundName
        pending = binder(sessionId, seed).then(async (bound) => {
          // The name is what a peer search finds; a conversation with no name
          // is addressable only by its run id.
          if (seed?.dir) await publish(bound, seed.dir)
          return bound
        })
        bindings.set(sessionId, pending)
      }
      return pending
    },
    dirFor(parent) {
      try {
        return options.dirFor?.(parent)
      } catch {
        return undefined
      }
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
      loops.set(sessionId, startInboundLoop({ sessionId, runtime, poll, pollIntervalMs, onError: options.onError }))
    },
    stop() {
      for (const loop of loops.values()) loop.stop()
      loops.clear()
    },
  }
  return runtime
}

async function defaultHostProvider(
  pendingAsks: PendingAskMap,
  control: () => Promise<{ client: MessagingClient; supervisorId: string }>,
): Promise<MessageProvider> {
  const { client, supervisorId } = await control()
  const { generation } = await client.messagePreflight()
  const provider = createHostMessageProvider({
    surface: 'dsh-host' as HostOptions['surface'],
    supervisorId,
    generation,
    client,
    pendingAsks,
    pollGeneration: () => 0,
  })
  installAskClaims(provider, new Map(), pendingAsks, () => 0)
  return provider
}

async function bindSession(
  control: { client: MessagingClient },
  sessionId: string,
): Promise<BoundConversation> {
  const bound = await control.client.messageBind({ host: 'dsh', session_id: sessionId })
  return { sessionId, runId: bound.run_id, brokerUrl: bound.broker_url, token: bound.run_token }
}

/**
 * Name the bound run the way pi names its host run, so a child can address the
 * harness by label instead of being told a run id.
 *
 * The label pairs the host with the working directory's repo (or its basename)
 * and takes a run-id tail when there is nothing better to distinguish it, which
 * is the same rule the other surfaces use.
 */
export async function publishBoundName(
  bound: BoundConversation,
  dir: string,
): Promise<string | undefined> {
  const toplevel = await gitToplevel(dir).catch(() => '')
  const label = defaultHostLabel({
    host: 'dsh',
    hostname: osHostname(),
    brokerUrl: bound.brokerUrl,
    dir,
    toplevel,
    runId: bound.runId,
    taken: null,
  })
  if (label === '') return undefined
  try {
    await publishHarnessName(
      { brokerUrl: bound.brokerUrl, runId: bound.runId, token: bound.token },
      label,
    )
    return label
  } catch {
    // A name that cannot be published leaves the run addressable by id; it is
    // not a reason to fail the bind.
    return undefined
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

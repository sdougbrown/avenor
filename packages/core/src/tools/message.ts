import { executeMessage } from '../message.js'
import type { MessageOutcome } from '../messaging-text.js'

export type MessageCallerKind = 'owner' | 'broker'
export type MessageSurface = 'pi-host' | 'pi-child' | 'opencode' | 'mcp-ts' | 'mcp' | 'channeltools' | 'sidecar'

/** One peers `list` entry: the tool's projection of the broker's SessionInfo. */
export interface PeerRecord {
  run_id: string
  label?: string
  kind?: string
  status?: string
  backend?: string
  model?: string
  dir?: string
}

/**
 * One routed call's outcome and the facts a surface reports. `text` is the
 * (action, outcome) render template and is set on every result, so no surface
 * composes a sentence and every failure says whether an ask can still be
 * withdrawn.
 */
export interface ProviderResult {
  action?: string
  outcome: MessageOutcome
  messageId?: string
  reply?: string
  text?: string
  runId?: string
  runtimeId?: string
  label?: string
  previousLabel?: string
  sentinelFile?: string
  onEvent?: string
  finalOutput?: string
  stopReason?: string
  truncated?: boolean
  hasRecord?: boolean
  /** A peers `list` transport's error text; set only on an incomplete listing. */
  detail?: string
}

export interface Resolution {
  runId: string
  publicRunId?: string
  match: 'unique' | 'ambiguous' | 'none'
  label?: string
  candidates?: string[]
}

export interface RunFacts {
  live: boolean
  resumable: boolean
  status: string
}

export interface PeersProviderResult {
  peers: PeerRecord[]
  /** Set when the transport returned peers together with an error. */
  detail?: string
}

/**
 * What one bound surface supplies to the router: who it speaks as, how it
 * resolves and reads state, and the action methods only it can perform.
 */
export interface MessageProvider {
  caller(): { kind: MessageCallerKind; runId: string }
  surface(): MessageSurface
  role(): string
  resolve(ref: string): Promise<Resolution>
  state(runId: string): Promise<RunFacts>
  tell(targetRunId: string, message: string): Promise<ProviderResult>
  ask(targetRunId: string, message: string, timeoutMs?: number): Promise<ProviderResult>
  reply(inboundAskId: string, message: string): Promise<ProviderResult>
  withdraw(messageId: string): Promise<ProviderResult>
  controlReply?(target: string, payload: unknown): Promise<ProviderResult>
  /** `target` is the RESOLVED runtime id; the provider mints runId and paths. */
  resume(req: {
    target: string
    message: string
    label?: string
    idempotencyKey?: string
  }): Promise<ProviderResult>
  rename(req: { sessionId?: string; name: string }): Promise<ProviderResult>
  last(runId: string): Promise<ProviderResult>
  peers(): Promise<PeersProviderResult>
  /** Host surfaces that own a turn loop publish their own finished turn. */
  recordLastTurn?(text: string, stopReason: string, truncated: boolean): Promise<void>
  claimAsk?(messageId: string): string | undefined
  restoreAsk?(messageId: string, askerRunId: string): void
  onResumed?(result: ProviderResult): void | Promise<void>
  registerResumed?(result: ProviderResult): void
}

/**
 * The one avenor_message handler: every surface routes through it, so the six
 * actions and their failure text exist once. A router tool error is thrown as a
 * typed Error, which each surface reports as its own error result.
 */
export async function messageTool(
  args: unknown,
  provider: MessageProvider,
): Promise<ProviderResult> {
  return executeMessage(provider, args)
}

/** Bind the handler to one provider. */
export function createMessageTool(
  provider: MessageProvider,
): (args: unknown) => Promise<ProviderResult> {
  return args => messageTool(args, provider)
}

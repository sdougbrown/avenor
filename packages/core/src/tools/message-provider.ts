import * as fs from 'node:fs'
import { randomUUID } from 'node:crypto'

import type {
  Client,
  MessageInbound,
  MessagePeersResult,
  MessageSendResult,
  MessageWaitResult,
  MessageWithdrawResult,
  ResumeResult,
  SessionLastResult,
  SessionRenameResult,
} from '../client.js'
import { RpcError } from '../client.js'
import { ensureRunPaths } from '../paths.js'
import { Supervisor, retainLiveIdentity, type RunInfo } from '../supervisor.js'
import { goQuote, newMsgID, type MessageOutcome } from '../messaging-text.js'
import { ToolError } from '../message.js'
import { findExternalRunById, listExternalRuns, registerExternalRun } from './run-registry.js'
import {
  RESUMABLE_SENTINEL_STATUSES,
  resumableFromSession,
  sentinelStatusToResult,
  TERMINAL_SENTINEL_STATUSES,
} from './sentinel-status.js'
import type {
  MessageProvider,
  MessageSurface,
  PeerRecord,
  PeersProviderResult,
  ProviderResult,
  Resolution,
  RunFacts,
} from './message.js'

/** The control-client surface a host provider needs, narrowed for testability. */
export interface MessageClient {
  messageSend(params: {
    to_run_id: string
    message_id: string
    generation: string
    body: string
    no_reply?: boolean
  }): Promise<MessageSendResult>
  messageWait(params: {
    message_id: string
    generation: string
    timeout_ms?: number
  }): Promise<MessageWaitResult>
  messageReply(params: {
    to_run_id: string
    message_id: string
    correlation_id?: string
    body: string
  }): Promise<MessageSendResult>
  messageWithdraw(params: { message_id: string; generation: string }): Promise<MessageWithdrawResult>
  messagePeers(params: { generation: string }): Promise<MessagePeersResult>
  resume(params: {
    run_id: string
    target: string
    message: string
    label?: string
    idempotency_key?: string
    sentinel_file?: string
    on_event?: string
  }): Promise<ResumeResult>
  sessionRename(sessionId: string, name: string): Promise<SessionRenameResult>
  sessionLast(runtimeId: string): Promise<SessionLastResult>
  sessionLastSet(
    runtimeId: string,
    finalOutput: string,
    stopReason: string,
    truncated: boolean,
  ): Promise<void>
  status(runtimeId?: string): Promise<Record<string, unknown>>
}

/** One route's answer from a child surface's inherited broker. */
export interface ChildBroker {
  tell(targetRunId: string, message: string, messageId: string): Promise<ProviderResult>
  ask(
    targetRunId: string,
    message: string,
    messageId: string,
    timeoutMs?: number,
    signal?: AbortSignal,
  ): Promise<ProviderResult>
  reply(askerRunId: string, inboundAskId: string, message: string): Promise<ProviderResult>
  withdraw(messageId: string): Promise<ProviderResult>
  peers(): Promise<PeersProviderResult>
  rename(req: { sessionId?: string; name: string }): Promise<ProviderResult>
  last(runId: string): Promise<ProviderResult>
}

export interface PendingAsk {
  from_run_id: string
  message_id: string
}

export const SUPERVISOR_RUN_ID = 'supervisor'
const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])

function firstNonEmpty(...values: Array<string | undefined>): string {
  for (const value of values) {
    if (value) return value
  }
  return ''
}

/** Strip the RPC framing so a model sees the control server's own sentence. */
function rpcText(error: RpcError): string {
  return error.message.replace(/^rpc error \[-?\d+\]: /, '')
}

function controlCode(error: unknown): string | undefined {
  if (!(error instanceof RpcError) || error.data === null || typeof error.data !== 'object') return undefined
  const code = (error.data as Record<string, unknown>).code
  return typeof code === 'string' && code ? code : undefined
}

function controlOutcome(error: unknown): string | undefined {
  if (!(error instanceof RpcError) || error.data === null || typeof error.data !== 'object') return undefined
  const outcome = (error.data as Record<string, unknown>).outcome
  return typeof outcome === 'string' && outcome ? outcome : undefined
}

function controlWireCode(error: unknown): string | undefined {
  if (!(error instanceof RpcError) || error.data === null || typeof error.data !== 'object') return undefined
  const code = (error.data as Record<string, unknown>).wire_code
  return typeof code === 'string' && code ? code : undefined
}

/** The broker rejected a receive because the caller's generation is stale. */
export function isGenerationMismatchError(error: unknown): boolean {
  if (error instanceof RpcError) {
    const data = error.data as Record<string, unknown> | null | undefined
    if (data?.code === 'generation-mismatch' || data?.wire_code === 'generation-mismatch') return true
  }
  return error instanceof Error && /generation mismatch/i.test(error.message)
}

/**
 * Matches Go's `statusReportsNoRuntime`: true only for the supervisor's absence
 * answer for this exact runtime, so a refusal or a transport failure stays the
 * caller's error. The sentence is rendered with `goQuote` because the
 * supervisor formats the id with Go's `%q`.
 */
function statusReportsNoRuntime(error: unknown, runId: string): boolean {
  if (!(error instanceof RpcError) || error.code !== -32000) return false
  return rpcText(error) === `runtime ${goQuote(runId)} not found`
}


/** Reject an abort exactly the way fetch does, so the ask cleanup path runs. */
function raceAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise
  if (signal.aborted) return Promise.reject(signal.reason ?? new Error('aborted'))
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason ?? new Error('aborted'))
    signal.addEventListener('abort', onAbort, { once: true })
    promise.then(
      value => {
        signal.removeEventListener('abort', onAbort)
        resolve(value)
      },
      error => {
        signal.removeEventListener('abort', onAbort)
        reject(error)
      },
    )
  })
}

const ROUTER_CODES = new Set([
  'ambiguous_name',
  'invalid_action',
  'invalid_name',
  'invalid_request',
  'not_permitted',
  'not_resumable',
  'refused',
  'target_not_terminal',
  'unknown_target',
  'unsupported_action',
  'wrong_action_for_state',
])

/** Map an authority failure onto the router's typed code, or a refusal. */
function authorityError(error: unknown): Error {
  if (error instanceof RpcError) {
    const code = controlCode(error)
    if (code && ROUTER_CODES.has(code)) return new ToolError(code, rpcText(error))
    return new ToolError('refused', rpcText(error))
  }
  return error instanceof Error ? error : new Error(String(error))
}

/**
 * The port of Go's `runstate.Translate` that classifies one supervisor status
 * record into the turn and resume facts the router gates on. `status.ts`'s
 * `translateStatus` is a different port — the sentinel-aware status renderer
 * for the status surface, which reads a sentinel file this path never does.
 */
function translateRunFacts(rawStatus: string, rawPhase: string): { status: string; live: boolean } {
  let status = rawStatus
  let phase = rawPhase
  switch (rawStatus) {
    case 'running':
      if (TERMINAL_STATUSES.has(rawPhase)) phase = ''
      break
    case 'idle':
      status = TERMINAL_STATUSES.has(rawPhase) ? rawPhase : 'running'
      break
    case 'ended':
      status = 'done'
      break
    case 'blocked':
      status = 'failed'
      break
    case 'done':
    case 'failed':
    case 'timeout':
    case 'killed':
    case 'waiting':
      break
    default:
      status = 'unknown'
  }
  void phase
  return { status, live: !TERMINAL_STATUSES.has(status) }
}

/**
 * Classify one broker peer row into the facts the router gates on. The Go
 * providers share this translation through `messaging.PeerRunFacts`: a peer row
 * carries no session and no sentinel, so the run is never resumable from here —
 * the supervisor that spawned it owns a resume.
 */
function peerFacts(peer: PeerRecord | undefined): RunFacts {
  const translation = translateRunFacts(peer?.status ?? '', '')
  return { live: translation.live, resumable: false, status: translation.status }
}

/** Read one sentinel file's status word, or undefined when the file is unreadable. */
async function readSentinelStatus(filePath: string): Promise<string | undefined> {
  try {
    const content = (await fs.promises.readFile(filePath, 'utf-8')).trim()
    return (content.split('\n')[0] ?? '').trim().toUpperCase()
  } catch {
    return undefined
  }
}

/**
 * Read one sentinel file's terminal result word, or undefined when the file is
 * unreadable or holds a status that is not terminal. The mapping is the status
 * surface's, so both classify a sentinel the same way.
 */
async function terminalSentinelStatus(filePath: string): Promise<string | undefined> {
  const status = await readSentinelStatus(filePath)
  if (!status || !TERMINAL_SENTINEL_STATUSES.has(status)) return undefined
  return sentinelStatusToResult(status)
}

async function sentinelResumable(filePath: string): Promise<boolean> {
  const status = await readSentinelStatus(filePath)
  return status !== undefined && RESUMABLE_SENTINEL_STATUSES.has(status)
}

function peerRecords(peers: MessagePeersResult): PeerRecord[] {
  return (peers.peers ?? []).map(peer => {
    const session = peer.session
    const decoded = session !== null && typeof session === 'object'
      ? (session as Record<string, unknown>)
      : undefined
    const record: PeerRecord = {
      run_id: typeof decoded?.run_id === 'string' && decoded.run_id ? decoded.run_id : peer.run_id,
    }
    for (const key of ['label', 'kind', 'status', 'backend', 'model', 'dir'] as const) {
      const value = decoded?.[key]
      if (typeof value === 'string' && value) record[key] = value
    }
    return record
  })
}

export interface HostProviderOptions {
  surface: MessageSurface
  supervisorId: string
  generation: string
  client: MessageClient
  /** The pending inbound asks the surface's poll fills; absent = no inbound. */
  pendingAsks?: Map<string, PendingAsk>
  /** The poll generation a restore is scoped to, so a restart cannot resurrect an ask. */
  pollGeneration?: () => number
  /** Runs after a successful resume registration; the surface's own tracking. */
  onResumed?: (result: ProviderResult) => void | Promise<void>
}

export type AskClaims = Map<string, { asker: string, generation: number }>

/** Install the claim/restore pair both factories share, so a restart cannot resurrect a stale ask. */
export function installAskClaims(
  provider: MessageProvider,
  claims: AskClaims,
  pendingAsks: Map<string, PendingAsk>,
  pollGeneration: (() => number) | undefined,
): void {
  provider.claimAsk = (messageId: string) => {
    const ask = pendingAsks.get(messageId)
    if (!ask) return undefined
    pendingAsks.delete(messageId)
    claims.set(messageId, { asker: ask.from_run_id, generation: pollGeneration?.() ?? 0 })
    return ask.from_run_id
  }
  provider.restoreAsk = (messageId: string, askerRunId: string) => {
    const claim = claims.get(messageId)
    claims.delete(messageId)
    if (claim && claim.generation === (pollGeneration?.() ?? 0)) {
      pendingAsks.set(messageId, { from_run_id: askerRunId, message_id: messageId })
    }
  }
  // A surface may own the claim map and install it after the factory did, so
  // the reply path releases through the pair rather than its local map.
  provider.releaseAsk = (messageId: string) => {
    claims.delete(messageId)
  }
}

/**
 * Build the bound host provider: one supervisor, one generation, one control
 * client. It never reads an ambient singleton or retargets, and it never
 * preflights per call. Listing-backed lookups fail fast: an incomplete
 * listing refuses every resolution and state read, even for a peer the
 * partial set contains, so a wire `peers_error` resolves the same way the Go
 * providers do. Self and public-ID resolution bypass the listing.
 */
export function createHostMessageProvider(options: HostProviderOptions): MessageProvider {
  const { surface, supervisorId, generation, client, pendingAsks, pollGeneration } = options
  const claims: AskClaims = new Map()

  async function listedPeers(): Promise<{ peers: PeerRecord[]; detail?: string }> {
    const result = await client.messagePeers({ generation })
    return { peers: peerRecords(result), detail: result.peers_error || undefined }
  }

  // A failed wait leaves no reader for the ask, so retract it the way
  // askclient.withdrawFailedWait does; the cleanup budget is best effort.
  async function retractAsk(messageId: string): Promise<void> {
    try {
      await client.messageWithdraw({ message_id: messageId, generation })
    } catch {
      // A failed cleanup must not replace the ask's own outcome.
    }
  }

  function publicRunIdFor(runId: string): string | undefined {
    const local = Supervisor.getInstance(supervisorId)?.getRunByRuntimeId(runId)
    if (local?.runId) return local.runId
    return listExternalRuns(supervisorId).find(info => info.runtimeId === runId)?.runId
  }

  function unique(info: RunInfo): Resolution {
    return {
      runId: info.runtimeId ?? '',
      publicRunId: info.runId,
      label: firstNonEmpty(info.label, info.runId),
      match: 'unique',
    }
  }

  // peerRunState reads one target's state from the broker's peer listing, which
  // is the only authority for a run this supervisor does not run. It answers
  // nothing for a listing that could not be read completely or that does not
  // hold the run, so the caller keeps the failure it already had. A stale
  // generation is not the listing's answer: the MCP surface retries the whole
  // call once after a re-preflight, and a mismatch swallowed here would report
  // the status failure instead of letting that retry run.
  async function peerRunState(runId: string): Promise<RunFacts | undefined> {
    let listed: { peers: PeerRecord[]; detail?: string }
    try {
      listed = await listedPeers()
    } catch (error) {
      if (isGenerationMismatchError(error)) throw error
      return undefined
    }
    if (listed.detail) return undefined
    const peer = listed.peers.find(entry => entry.run_id === runId)
    return peer ? peerFacts(peer) : undefined
  }

  const provider: MessageProvider = {
    caller: () => ({ kind: 'owner', runId: SUPERVISOR_RUN_ID }),
    surface: () => surface,
    role: () => 'host',

    async resolve(ref: string): Promise<Resolution> {
      if (ref === '') return { runId: '', match: 'none' }
      if (ref === SUPERVISOR_RUN_ID) {
        return { runId: SUPERVISOR_RUN_ID, label: SUPERVISOR_RUN_ID, match: 'unique' }
      }
      // The public-id plane is ID-only: never an alias map or a label.
      const instance = Supervisor.getInstance(supervisorId)
      const local = instance ? instance.getRunById(ref) : undefined
      if (local?.runtimeId) return unique(local)
      if (!instance) {
        const external = findExternalRunById(supervisorId, ref)
        if (external?.runtimeId) return unique(external)
      }
      const { peers, detail } = await listedPeers()
      if (detail) {
        // The listing was incomplete, so any match it holds may be shadowed by
        // a peer it missed. Refuse the whole lookup the way the Go providers
        // refuse an incomplete listing, and carry the transport's detail.
        throw new Error(`the peer listing is incomplete: ${detail}`)
      }
      const exact = peers.find(peer => peer.run_id === ref)
      if (exact) {
        return {
          runId: exact.run_id,
          publicRunId: publicRunIdFor(exact.run_id),
          label: firstNonEmpty(exact.label, exact.run_id),
          match: 'unique',
        }
      }
      const candidates = peers.filter(peer => peer.label === ref).map(peer => peer.run_id)
      if (candidates.length === 1) {
        return {
          runId: candidates[0],
          publicRunId: publicRunIdFor(candidates[0]),
          label: ref,
          match: 'unique',
        }
      }
      if (candidates.length > 1) return { runId: '', match: 'ambiguous', candidates }
      return { runId: '', match: 'none' }
    },

    async state(runId: string): Promise<RunFacts> {
      if (runId === SUPERVISOR_RUN_ID) {
        const { peers, detail } = await listedPeers()
        if (detail) {
          // The listing was incomplete, so the status it holds may be stale or
          // shadowed. Refuse the read the way the Go providers do.
          throw new Error(`the peer listing is incomplete: ${detail}`)
        }
        const peer = peers.find(entry => entry.run_id === SUPERVISOR_RUN_ID)
        return peerFacts(peer)
      }
      let status: Record<string, unknown>
      try {
        status = await client.status(runId)
      } catch (error) {
        // The status verb answers only for the runs this supervisor runs, so a
        // run a registered child broker owns has no runtime here and reports
        // from the broker's listing instead. Only that absence answer is read
        // through the listing: a denial or a transport failure is this call's
        // outcome and stays the caller's error.
        if (!statusReportsNoRuntime(error, runId)) throw error
        // Mirrors the Go rung ahead of the listing: a run that settled before
        // a supervisor restart keeps its outcome discoverable by run id. The
        // fallback stays narrow — a run neither plane holds, an unreadable
        // sentinel, or a non-terminal one keeps the absence answer.
        const instance = Supervisor.getInstance(supervisorId)
        const registered = instance?.getRunById(runId) ?? findExternalRunById(supervisorId, runId)
        if (registered) {
          const terminal = await terminalSentinelStatus(registered.sentinelPath)
          if (terminal) {
            return {
              live: false,
              resumable: resumableFromSession(registered.sessionId),
              status: terminal,
            }
          }
        }
        const listed = await peerRunState(runId)
        if (listed) return listed
        throw error
      }
      const translation = translateRunFacts(
        typeof status.status === 'string' ? status.status : '',
        typeof status.phase === 'string' ? status.phase : '',
      )
      let resumable = resumableFromSession(status.session_id)
      if (!resumable && typeof status.sentinel_file === 'string') {
        resumable = await sentinelResumable(status.sentinel_file)
      }
      return { live: translation.live, resumable, status: translation.status }
    },

    async tell(targetRunId: string, message: string): Promise<ProviderResult> {
      const messageId = newMsgID()
      const body = JSON.stringify({
        from: SUPERVISOR_RUN_ID,
        from_run_id: SUPERVISOR_RUN_ID,
        to_run_id: targetRunId,
        message,
        role: 'host',
      })
      let result: MessageSendResult
      try {
        result = await client.messageSend({
          to_run_id: targetRunId,
          message_id: messageId,
          generation,
          body,
          no_reply: true,
        })
      } catch (error) {
        const outcome = controlOutcome(error)
        if (outcome) return { outcome: outcome as MessageOutcome, messageId }
        throw authorityError(error)
      }
      return {
        outcome: result.outcome as MessageOutcome,
        messageId: firstNonEmpty(result.message_id, messageId),
      }
    },

    async ask(
      targetRunId: string,
      message: string,
      timeoutMs?: number,
      signal?: AbortSignal,
    ): Promise<ProviderResult> {
      const messageId = newMsgID()
      const body = JSON.stringify({
        id: messageId,
        from: SUPERVISOR_RUN_ID,
        from_run_id: SUPERVISOR_RUN_ID,
        to_run_id: targetRunId,
        message,
        role: 'host',
        expects_reply: true,
      })
      let sent: MessageSendResult
      try {
        sent = await client.messageSend({
          to_run_id: targetRunId,
          message_id: messageId,
          generation,
          body,
        })
      } catch (error) {
        const outcome = controlOutcome(error)
        if (outcome) return { outcome: outcome as MessageOutcome, messageId }
        throw authorityError(error)
      }
      if (sent.outcome === 'not-sent' || sent.outcome === 'auth-rejected') {
        return { outcome: sent.outcome as MessageOutcome, messageId: firstNonEmpty(sent.message_id, messageId) }
      }
      let waited: MessageWaitResult
      try {
        waited = await raceAbort(client.messageWait({
          message_id: messageId,
          generation,
          ...(timeoutMs !== undefined ? { timeout_ms: timeoutMs } : {}),
        }), signal)
      } catch (error) {
        await retractAsk(messageId)
        if (signal?.aborted) return { outcome: 'possibly-pending', messageId }
        const outcome = controlOutcome(error)
        if (!outcome) throw authorityError(error)
        const carried = error instanceof RpcError && error.data && typeof error.data === 'object'
          ? (error.data as Record<string, unknown>).message_id
          : undefined
        return {
          outcome: outcome as MessageOutcome,
          messageId: typeof carried === 'string' && carried ? carried : messageId,
        }
      }
      // A wait that ended without a reply leaves the ask unread; retract it the
      // way askclient does, so a timed-out target does not keep a live edge.
      if (waited.outcome !== 'replied') await retractAsk(messageId)
      const result: ProviderResult = {
        outcome: waited.outcome as MessageOutcome,
        messageId: firstNonEmpty(waited.message_id, messageId),
      }
      if (waited.outcome === 'replied') result.reply = renderReply(waited.reply)
      return result
    },

    async reply(inboundAskId: string, message: string, askerRunId: string): Promise<ProviderResult> {
      const body = JSON.stringify({ message, role: 'host' })
      let sent: MessageSendResult
      try {
        sent = await client.messageReply({
          to_run_id: askerRunId,
          message_id: inboundAskId,
          correlation_id: inboundAskId,
          body,
        })
      } catch (error) {
        const outcome = controlOutcome(error)
        if (outcome) return { outcome: outcome as MessageOutcome, messageId: inboundAskId }
        throw authorityError(error)
      }
      if (sent.outcome === 'replied') provider.releaseAsk?.(inboundAskId)
      return { outcome: sent.outcome as MessageOutcome, messageId: inboundAskId }
    },

    async withdraw(messageId: string): Promise<ProviderResult> {
      try {
        const withdrawn = await client.messageWithdraw({ message_id: messageId, generation })
        return { outcome: withdrawn.outcome as MessageOutcome }
      } catch (error) {
        const outcome = controlOutcome(error)
        if (outcome) {
          // An absence fence that could not be installed leaves a delayed send
          // admissible; claiming nothing is pending would be a lie.
          if (controlWireCode(error) === 'fence-capacity' && outcome === 'edge-absent') {
            return { outcome: 'possibly-pending' }
          }
          return { outcome: outcome as MessageOutcome }
        }
        throw authorityError(error)
      }
    },

    async resume(req): Promise<ProviderResult> {
      const runId = randomUUID()
      const paths = ensureRunPaths(runId)
      let resumed: ResumeResult
      try {
        resumed = await client.resume({
          run_id: runId,
          target: req.target,
          message: req.message,
          ...(req.label !== undefined ? { label: req.label } : {}),
          ...(req.idempotencyKey !== undefined ? { idempotency_key: req.idempotencyKey } : {}),
          sentinel_file: paths.sentinelPath,
          on_event: paths.eventLogPath,
        })
      } catch (error) {
        // A resume that never published a run leaves nothing but the directory it
        // needed; remove it, but only while it is still empty, so a call whose reply
        // was lost cannot delete a live run's artifacts.
        try {
          if (fs.readdirSync(paths.runDir).length === 0) fs.rmdirSync(paths.runDir)
        } catch {
          // Best effort: the directory is the only thing at stake.
        }
        throw error
      }
      return {
        outcome: 'resumed',
        runId: resumed.run_id,
        runtimeId: resumed.runtime_id,
        label: resumed.label,
        sessionId: resumed.session_id,
        sentinelFile: resumed.sentinel_file,
        onEvent: resumed.on_event,
      }
    },

    async rename(req): Promise<ProviderResult> {
      let renamed: SessionRenameResult
      try {
        renamed = await client.sessionRename(req.sessionId ?? '', req.name)
      } catch (error) {
        throw authorityError(error)
      }
      return {
        outcome: 'renamed',
        runId: renamed.run_id,
        label: renamed.label,
        previousLabel: renamed.previous_label,
      }
    },

    async last(runId: string): Promise<ProviderResult> {
      let last: SessionLastResult
      try {
        last = await client.sessionLast(runId)
      } catch (error) {
        throw authorityError(error)
      }
      return {
        outcome: 'sent',
        label: last.label,
        hasRecord: last.has_record,
        finalOutput: last.final_output,
        stopReason: last.stop_reason,
        truncated: last.final_output_truncated,
      }
    },

    async peers(): Promise<PeersProviderResult> {
      const { peers, detail } = await listedPeers()
      return { peers, detail }
    },

    async recordLastTurn(text: string, stopReason: string, truncated: boolean): Promise<void> {
      await client.sessionLastSet(SUPERVISOR_RUN_ID, text, stopReason, truncated)
    },

    async registerResumed(result: ProviderResult): Promise<void> {
      const instance = Supervisor.getInstance(supervisorId)
      const info: RunInfo = {
        runId: result.runId ?? '',
        label: result.label ?? '',
        sentinelPath: result.sentinelFile ?? '',
        eventLogPath: result.onEvent ?? '',
        runtimeId: result.runtimeId,
        sessionId: result.sessionId,
      }
      // The resumed runtime reports the identity the authority carried over;
      // a status failure keeps the resume but leaves those fields unset.
      if (result.runtimeId) {
        try {
          const status = await client.status(result.runtimeId)
          retainLiveIdentity(info, status)
          const value = (key: string) => (typeof status[key] === 'string' && status[key] ? status[key] as string : undefined)
          info.dir = value('dir')
          info.thinking = value('thinking') as RunInfo['thinking']
          if (typeof status.auto_approve === 'boolean') info.autoApprove = status.auto_approve
        } catch {
          // Registration still keeps the run resolvable by id and label.
        }
      }
      if (instance) {
        instance.registerResumed(info)
        return
      }
      registerExternalRun({ ...info, supervisorId })
    },

    onResumed: options.onResumed,
  }

  if (pendingAsks) installAskClaims(provider, claims, pendingAsks, pollGeneration)

  return provider
}

/**
 * Render a wait result's reply body the way askclient.RenderReply does: a
 * `{message}` payload is the text, a payload the wire left out is
 * `(empty reply)`, and anything else is the full envelope with the payload
 * passed through. A payload that is JSON `null` or an empty string is present,
 * so it renders the envelope rather than claiming the reply was empty.
 */
function renderReply(reply: MessageInbound | undefined): string {
  if (!reply) return ''
  let payload = reply.payload
  if (typeof payload === 'string') {
    try {
      payload = JSON.parse(payload)
    } catch {
      payload = undefined
      return JSON.stringify({
        from_run_id: reply.from_run_id,
        message_id: reply.message_id,
        payload: reply.payload,
      })
    }
  }
  if (payload === undefined) return '(empty reply)'
  if (payload !== null && typeof payload === 'object') {
    const message = (payload as Record<string, unknown>).message
    if (typeof message === 'string' && message) return message
  }
  return JSON.stringify({
    from_run_id: reply.from_run_id,
    message_id: reply.message_id,
    payload,
  })
}

export interface ChildProviderOptions {
  surface: MessageSurface
  runId: string
  broker: ChildBroker
  /** The pending inbound asks the child's poll fills. */
  pendingAsks?: Map<string, PendingAsk>
  /** The poll generation a restore is scoped to, so a restart cannot resurrect an ask. */
  pollGeneration?: () => number
}

/**
 * Build a child provider over the surface's inherited broker. A child never
 * learns a public run id, and it cannot read a sentinel, so it is never
 * resumable from here: the supervisor owns a resume. Listing-backed lookups
 * fail fast: an incomplete listing refuses every resolution and state read,
 * even for a peer the partial set contains, so a wire `peers_error` resolves
 * the same way the Go providers do. Self resolution bypasses the listing.
 */
export function createChildMessageProvider(options: ChildProviderOptions): MessageProvider {
  const { surface, runId, broker, pendingAsks, pollGeneration } = options
  const claims: AskClaims = new Map()

  async function listedPeers(): Promise<{ peers: PeerRecord[]; detail?: string }> {
    const result = await broker.peers()
    return { peers: result.peers, detail: result.detail }
  }

  const provider: MessageProvider = {
    caller: () => ({ kind: 'broker', runId }),
    surface: () => surface,
    role: () => 'agent',

    async resolve(ref: string): Promise<Resolution> {
      if (ref === '') return { runId: '', match: 'none' }
      if (ref === runId) return { runId, label: ref, match: 'unique' }
      const { peers, detail } = await listedPeers()
      if (detail) {
        // The listing was incomplete, so any match it holds may be shadowed by
        // a peer it missed. Refuse the whole lookup the way the Go providers
        // refuse an incomplete listing, and carry the transport's detail.
        throw new Error(`the peer listing is incomplete: ${detail}`)
      }
      const exact = peers.find(peer => peer.run_id === ref)
      if (exact) return { runId: exact.run_id, label: firstNonEmpty(exact.label, exact.run_id), match: 'unique' }
      const candidates = peers.filter(peer => peer.label === ref).map(peer => peer.run_id)
      if (candidates.length === 1) return { runId: candidates[0], label: ref, match: 'unique' }
      if (candidates.length > 1) return { runId: '', match: 'ambiguous', candidates }
      return { runId: '', match: 'none' }
    },

    async state(targetRunId: string): Promise<RunFacts> {
      const { peers, detail } = await listedPeers()
      if (detail) {
        // The listing was incomplete, so the status it holds may be stale or
        // shadowed. Refuse the read the way the Go providers do.
        throw new Error(`the peer listing is incomplete: ${detail}`)
      }
      const peer = peers.find(entry => entry.run_id === targetRunId)
      if (!peer) {
        throw new Error(`run ${targetRunId} is not visible`)
      }
      return peerFacts(peer)
    },

    tell: (targetRunId, message) => broker.tell(targetRunId, message, newMsgID()),
    ask: (targetRunId, message, timeoutMs, signal) => broker.ask(targetRunId, message, newMsgID(), timeoutMs, signal),
    reply: async (inboundAskId, message, askerRunId) => {
      const result = await broker.reply(askerRunId, inboundAskId, message)
      if (result.outcome === 'replied') provider.releaseAsk?.(inboundAskId)
      return result
    },
    withdraw: messageId => broker.withdraw(messageId),
    resume: async () => {
      throw new ToolError(
        'not_permitted',
        'a broker-authenticated run may resume only a run it spawned, which a child cannot own',
      )
    },
    rename: req => broker.rename(req),
    last: runIdToRead => broker.last(runIdToRead),
    peers: () => broker.peers(),
  }

  if (pendingAsks) installAskClaims(provider, claims, pendingAsks, pollGeneration)

  return provider
}

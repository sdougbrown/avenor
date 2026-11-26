import { Supervisor, killTool, resultTool, spawnTool } from '@dougbots/avenor-core'
import { toSubagentResult, type AvenorTerminal } from './stop-reason.js'
import {
  permissionNotice,
  toPendingPermission,
  type PendingPermission,
  type RawPendingPermission,
} from './permission.js'
import {
  NO_START_CAPABILITIES,
  type ContentBlock,
  type ParentAgent,
  type SubagentProvider,
  type SubagentRun,
  type SubagentStartRequest,
  type UserMessageLike,
} from './types.js'

/** Wiring the spike needs from a real broker. Kept behind a seam so the
 * provider's contract can be tested without a running `avenor`. */
export interface AvenorSeam {
  /**
   * Start one run. `onPending` fires for each permission request the run blocks
   * on, at most once per request id; the seam keeps waiting until the run is
   * terminal, so the caller sees every block and no duplicate notices.
   */
  spawn(
    request: {
      readonly prompt: string
      readonly cwd?: string
      readonly label?: string
    },
    onPending: (runId: string, permission: PendingPermission) => void,
  ): Promise<AvenorHandle>
}

/**
 * What the plugin tracks outside one run: which agent started it (so an inbound
 * frame can reach that agent) and whether the receive loop is running.
 */
export interface DelegationTracker {
  remember(runId: string, parent: ParentAgent): void
  forget(runId: string): void
  ensureInbound(): void
}

export interface AvenorHandle {
  readonly runId: string
  /** Resolves once the run is terminal — never while it waits for a decision. */
  readonly done: Promise<AvenorTerminal>
  cancel(): Promise<void>
}

/** How a delegated child picks its runtime. Without one of these Avenor runs
 * its default agent, which is rarely what the parent model is running. */
export interface AvenorSelection {
  readonly agent?: string
  readonly model?: string
  readonly backend?: string
  readonly rosterFile?: string
  readonly rosterEntry?: string
}

/**
 * The broker calls the seam needs. Injected so the seam's own lifecycle rules
 * — one kill per run, none after settlement, one notice per permission — are
 * testable without a running `avenor`; the defaults are core's tools.
 */
export interface CoreTools {
  spawn(request: {
    readonly prompt: string
    readonly dir?: string
    readonly label?: string
    readonly agent?: string
    readonly model?: string
    readonly backend?: string
    readonly rosterFile?: string
    readonly rosterEntry?: string
    readonly messaging?: boolean
    readonly parent_run_id?: string
  }): Promise<{ readonly run_id: string }>
  result(request: { readonly runId: string; readonly wait: boolean }): Promise<{
    readonly status: string
    readonly ready: boolean
    readonly stop_reason?: string
    readonly output?: string
    readonly timed_out?: boolean
    readonly pending_permission?: RawPendingPermission
  }>
  kill(request: { readonly runId: string }): Promise<unknown>
}

const coreTools: CoreTools = {
  // The supervisor owns the broker process; starting it belongs to the real
  // implementation, not to the seam's lifecycle rules.
  async spawn(request) {
    await Supervisor.get()
    return spawnTool(request)
  },
  result: (request) => resultTool(request),
  kill: (request) => killTool(request),
}

export interface CoreSeamOptions {
  readonly selection?: AvenorSelection
  readonly tools?: CoreTools
  readonly pollIntervalMs?: number
  readonly sleep?: (ms: number) => Promise<void>
  /** Make the child addressable as `parentRunId` in the broker. */
  readonly messaging?: boolean
  /** The run id a messaging-enabled child addresses; the harness is `supervisor`. */
  readonly parentRunId?: string
}

function selectionParentRunId(parentRunId: string | undefined): { parent_run_id?: string } {
  return parentRunId === undefined ? {} : { parent_run_id: parentRunId }
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

function terminalOf(runId: string, result: {
  readonly status: string
  readonly ready: boolean
  readonly stop_reason?: string
  readonly output?: string
  readonly timed_out?: boolean
}): AvenorTerminal {
  return {
    runId,
    status: result.status,
    ready: result.ready,
    ...(result.stop_reason === undefined ? {} : { stopReason: result.stop_reason }),
    ...(result.output === undefined ? {} : { output: result.output }),
    ...(result.timed_out === undefined ? {} : { timedOut: result.timed_out }),
  }
}

/** Join the prompt's text blocks; DSH content also carries images, which the
 * spike drops rather than silently mangling. */
export function promptText(prompt: readonly ContentBlock[]): string {
  return prompt
    .filter((block): block is { type: 'text'; text: string } => block.type === 'text')
    .map((block) => block.text)
    .join('\n')
}

/** The real seam: core's tools against the singleton supervisor. */
export function createCoreSeam(options: CoreSeamOptions = {}): AvenorSeam {
  const selection = options.selection ?? {}
  const tools = options.tools ?? coreTools
  const pollIntervalMs = options.pollIntervalMs ?? 1000
  const sleep = options.sleep ?? defaultSleep

  return {
    async spawn(request, onPending) {
      const spawned = await tools.spawn({
        prompt: request.prompt,
        ...(request.cwd === undefined ? {} : { dir: request.cwd }),
        ...(request.label === undefined ? {} : { label: request.label }),
        ...(selection.agent === undefined ? {} : { agent: selection.agent }),
        ...(selection.model === undefined ? {} : { model: selection.model }),
        ...(selection.backend === undefined ? {} : { backend: selection.backend }),
        ...(selection.rosterFile === undefined ? {} : { rosterFile: selection.rosterFile }),
        ...(selection.rosterEntry === undefined ? {} : { rosterEntry: selection.rosterEntry }),
        ...(options.messaging === true
          ? { messaging: true, ...(selectionParentRunId(options.parentRunId)) }
          : {}),
      })

      const runId = spawned.run_id

      // `resultTool` returns early while a run is `waiting`, so a permission
      // request is a distinct outcome rather than a terminal error. Blocking
      // `result` never settles while the run waits, so the loop polls the
      // non-blocking form until the decision clears the request.
      let settled = false
      const done = (async (): Promise<AvenorTerminal> => {
        while (true) {
          const result = await tools.result({ runId, wait: true })
          if (result.ready) {
            settled = true
            return terminalOf(runId, result)
          }
          const raw = result.pending_permission
          if (!raw) {
            settled = true
            return terminalOf(runId, result)
          }

          const pending = toPendingPermission(raw)
          onPending(runId, pending)

          // Wait for this request to be answered or replaced.
          let current = pending.requestId
          while (true) {
            await sleep(pollIntervalMs)
            const next = await tools.result({ runId, wait: false })
            if (next.ready) {
              settled = true
              return terminalOf(runId, next)
            }
            const nextPending = next.pending_permission
            if (!nextPending) break
            if ((nextPending.request_id ?? '') === current) continue
            current = nextPending.request_id ?? ''
            break
          }
        }
      })()

      let killed = false
      return {
        runId,
        done,
        async cancel() {
          if (killed || settled) return
          killed = true
          try {
            await tools.kill({ runId })
          } catch {
            // Already terminal, or the broker went away: disposal still
            // succeeds because there is nothing left to stop.
          }
        },
      }
    },
  }
}

/**
 * Tell the spawning agent it has a decision to make. A running parent gets the
 * notice mid-turn; an idle one is woken with a new turn, because an unanswered
 * request would otherwise stall the run until Avenor's own timeout.
 */
export function notifyParent(
  parent: ParentAgent,
  runId: string,
  permission: PendingPermission,
): void {
  const message = permissionMessage(runId, permission)
  if (parent.status === 'running') parent.inject(message)
  else parent.followup(message)
}

/** Mirrors `createUserMessage` from `@deepseek-ai/dsh-llm` without importing it. */
export function permissionMessage(runId: string, permission: PendingPermission): UserMessageLike {
  return {
    id: crypto.randomUUID(),
    role: 'user',
    content: [{ type: 'text', text: permissionNotice(runId, permission) }],
    source: { kind: 'user' },
  }
}


/**
 * The provider DSH's subagent service registers. It owns one external Avenor
 * run per start, wakes the parent on a permission request, and maps the run's
 * terminal state onto the subagent result.
 */
export function createAvenorProvider(
  seam: AvenorSeam,
  tracker?: DelegationTracker,
): SubagentProvider {
  return {
    name: 'avenor',
    capabilities: NO_START_CAPABILITIES,
    inheritsParentContext: false,

    async start(request: SubagentStartRequest): Promise<SubagentRun> {
      const handle = await seam.spawn(
        {
          prompt: promptText(request.prompt),
          ...(request.cwd === undefined ? {} : { cwd: request.cwd }),
          ...(request.label === undefined ? {} : { label: request.label }),
        },
        (runId, permission) => notifyParent(request.parent, runId, permission),
      )

      tracker?.remember(handle.runId, request.parent)
      tracker?.ensureInbound()

      const onAbort = (): void => {
        void handle.cancel()
      }
      request.signal.addEventListener('abort', onAbort, { once: true })

      return {
        id: handle.runId,
        result: handle.done.then((terminal) => {
          // A settled child sends nothing more; forget it before the manager
          // disposes, so a late frame cannot reach an agent that moved on.
          tracker?.forget(handle.runId)
          return toSubagentResult(terminal)
        }),
        dispose: async () => {
          request.signal.removeEventListener('abort', onAbort)
          await handle.cancel()
          tracker?.forget(handle.runId)
        },
      }
    },
  }
}

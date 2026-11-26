import { Supervisor, killTool, resultTool, spawnTool } from '@dougbots/avenor-core'
import { toSubagentResult, type AvenorTerminal } from './stop-reason.js'
import {
  NO_START_CAPABILITIES,
  type ContentBlock,
  type SubagentProvider,
  type SubagentRun,
  type SubagentStartRequest,
} from './types.js'

/** Wiring the spike needs from a real broker. Kept behind a seam so the
 * provider's contract can be tested without a running `avenor`. */
export interface AvenorSeam {
  spawn(request: {
    readonly prompt: string
    readonly cwd?: string
    readonly label?: string
  }): Promise<AvenorHandle>
}

export interface AvenorHandle {
  readonly runId: string
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

/** Join the prompt's text blocks; DSH content also carries images, which the
 * spike drops rather than silently mangling. */
export function promptText(prompt: readonly ContentBlock[]): string {
  return prompt
    .filter((block): block is { type: 'text'; text: string } => block.type === 'text')
    .map((block) => block.text)
    .join('\n')
}

/**
 * The broker calls the seam needs. Injected so the seam's own lifecycle rules
 * — one kill per run and none after settlement — are testable without a
 * running `avenor`; the defaults are core's tools.
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
  }): Promise<{ readonly run_id: string }>
  result(request: { readonly runId: string; readonly wait: boolean }): Promise<{
    readonly status: string
    readonly ready: boolean
    readonly stop_reason?: string
    readonly output?: string
    readonly timed_out?: boolean
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

/** The real seam: core's tools against the singleton supervisor. */
export function createCoreSeam(
  selection: AvenorSelection = {},
  tools: CoreTools = coreTools,
): AvenorSeam {
  return {
    async spawn(request) {
      const spawned = await tools.spawn({
        prompt: request.prompt,
        ...(request.cwd === undefined ? {} : { dir: request.cwd }),
        ...(request.label === undefined ? {} : { label: request.label }),
        ...(selection.agent === undefined ? {} : { agent: selection.agent }),
        ...(selection.model === undefined ? {} : { model: selection.model }),
        ...(selection.backend === undefined ? {} : { backend: selection.backend }),
        ...(selection.rosterFile === undefined ? {} : { rosterFile: selection.rosterFile }),
        ...(selection.rosterEntry === undefined ? {} : { rosterEntry: selection.rosterEntry }),
      })

      const runId = spawned.run_id
      // `dispose()` runs after `result` has already resolved, so the seam has
      // to remember settlement: killing a finished run records a second
      // `session.end` as `cancelled` and makes a successful run look stopped.
      let settled = false
      const done = tools.result({ runId, wait: true }).then(
        (result): AvenorTerminal => {
          settled = true
          return {
            runId,
            status: result.status,
            ready: result.ready,
            ...(result.stop_reason === undefined ? {} : { stopReason: result.stop_reason }),
            ...(result.output === undefined ? {} : { output: result.output }),
            ...(result.timed_out === undefined ? {} : { timedOut: result.timed_out }),
          }
        },
      )

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
 * The provider DSH's subagent service registers. It owns one external Avenor
 * run per start and maps its terminal state onto the subagent result.
 */
export function createAvenorProvider(seam: AvenorSeam): SubagentProvider {
  return {
    name: 'avenor',
    capabilities: NO_START_CAPABILITIES,
    inheritsParentContext: false,

    async start(request: SubagentStartRequest): Promise<SubagentRun> {
      const handle = await seam.spawn({
        prompt: promptText(request.prompt),
        ...(request.cwd === undefined ? {} : { cwd: request.cwd }),
        ...(request.label === undefined ? {} : { label: request.label }),
      })

      const onAbort = (): void => {
        void handle.cancel()
      }
      request.signal.addEventListener('abort', onAbort, { once: true })

      return {
        id: handle.runId,
        result: handle.done.then(toSubagentResult),
        dispose: async () => {
          request.signal.removeEventListener('abort', onAbort)
          await handle.cancel()
        },
      }
    },
  }
}

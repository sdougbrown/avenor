import { answerPermissionTool } from '@dougbots/avenor-core'
import { answerToolDefinition, type AnswerDeps } from './answer-tool.js'
import { controlToolDefinitions, type ControlTools } from './control-tools.js'
import { messageToolDefinition, peersToolDefinition } from './message-tools.js'
import { createMessagingRuntime, type MessagingRuntime } from './messaging.js'
import {
  createAvenorProvider,
  createCoreSeam,
  type AvenorSeam,
  type AvenorSelection,
} from './provider.js'
import type { HostContext, ParentAgent } from './types.js'

/** Wait for both services this plugin registers into. */
export const inject = ['subagents', 'tools']

/**
 * The bundle row's `config`. Every field names how the delegated child runs;
 * anything omitted is left to Avenor's own defaults, which means the harness's
 * own roster and environment decide.
 */
export interface HostConfig {
  readonly agent?: string
  readonly model?: string
  readonly backend?: string
  readonly rosterFile?: string
  readonly rosterEntry?: string
  /**
   * Bind each session's conversation and make its children messaging children
   * of it. Defaults on; `false` leaves the spawns one-shot and the tools
   * send-only.
   */
  readonly messaging?: boolean
}

/** Builds the broker seam for one selection; injectable for tests. */
export type SeamFactory = (selection: AvenorSelection) => AvenorSeam

const defaultAnswer: AnswerDeps = {
  answer: (request) => answerPermissionTool(request),
}

const defaultSeamFactory: SeamFactory = (selection) => createCoreSeam({ selection })

function selectionOf(config: HostConfig): AvenorSelection {
  return {
    ...(config.agent === undefined ? {} : { agent: config.agent }),
    ...(config.model === undefined ? {} : { model: config.model }),
    ...(config.backend === undefined ? {} : { backend: config.backend }),
    ...(config.rosterFile === undefined ? {} : { rosterFile: config.rosterFile }),
    ...(config.rosterEntry === undefined ? {} : { rosterEntry: config.rosterEntry }),
  }
}

export interface RegisterDeps {
  readonly answer?: AnswerDeps
  readonly makeSeam?: SeamFactory
  readonly runtime?: MessagingRuntime
  readonly control?: ControlTools
}

export function register(
  ctx: HostContext,
  config: HostConfig = {},
  deps: RegisterDeps = {},
): MessagingRuntime {
  const answer = deps.answer ?? defaultAnswer
  const makeSeam = deps.makeSeam ?? defaultSeamFactory
  const messaging = config.messaging !== false
  const runtime = deps.runtime ?? createMessagingRuntime({ dirFor: (parent) => workingDirectoryOf(ctx, parent) })

  ctx.subagents.registerProvider(
    createAvenorProvider(makeSeam(selectionOf(config)), messaging ? runtime : undefined),
  )
  ctx.tools.register(answerToolDefinition(answer))
  ctx.tools.register(messageToolDefinition(runtime))
  ctx.tools.register(peersToolDefinition(runtime))
  for (const definition of controlToolDefinitions(deps.control)) ctx.tools.register(definition)
  return runtime
}

/** The parent's directory, for naming its conversation; falls back to the host cwd. */
function workingDirectoryOf(ctx: HostContext, parent: ParentAgent): string | undefined {
  const service = ctx.workingDirectory
  if (!service) return process.cwd()
  try {
    const dir = service.get(parent.session)
    return dir === '' ? process.cwd() : dir
  } catch {
    return process.cwd()
  }
}

export function apply(ctx: HostContext, config: HostConfig = {}, deps: RegisterDeps = {}): void {
  const runtime = register(ctx, config, deps)
  // Cordis runs the effect callback now and its returned disposer on unload.
  if (ctx.effect) ctx.effect(() => () => runtime.stop())
}

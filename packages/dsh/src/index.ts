import { answerPermissionTool } from '@dougbots/avenor-core'
import { answerToolDefinition, type AnswerDeps } from './answer-tool.js'
import { messageToolDefinition, peersToolDefinition, type LazyMessagingHost } from './message-tools.js'
import { createLazyMessagingHost, HOST_RUN_ID } from './messaging.js'
import {
  createAvenorProvider,
  createCoreSeam,
  type AvenorSeam,
  type AvenorSelection,
} from './provider.js'
import type { HostContext } from './types.js'

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
  /** Make each delegated child addressable in Avenor's broker. */
  readonly messaging?: boolean
  /** The run id a messaging-enabled child addresses; defaults to `supervisor`. */
  readonly parentRunId?: string
}

export interface SeamRequest {
  readonly selection: AvenorSelection
  readonly messaging: boolean
  readonly parentRunId: string
}

/** Builds the broker seam for one start; injectable for tests. */
export type SeamFactory = (request: SeamRequest) => AvenorSeam

const defaultAnswer: AnswerDeps = {
  answer: (request) => answerPermissionTool(request),
}

const defaultSeamFactory: SeamFactory = ({ selection, messaging, parentRunId }) =>
  createCoreSeam({
    selection,
    ...(messaging ? { messaging: true, parentRunId } : {}),
  })

function selectionOf(config: HostConfig): AvenorSelection {
  return {
    ...(config.agent === undefined ? {} : { agent: config.agent }),
    ...(config.model === undefined ? {} : { model: config.model }),
    ...(config.backend === undefined ? {} : { backend: config.backend }),
    ...(config.rosterFile === undefined ? {} : { rosterFile: config.rosterFile }),
    ...(config.rosterEntry === undefined ? {} : { rosterEntry: config.rosterEntry }),
  }
}

/**
 * Register the provider and the answer tool. Kept separate from `apply` so
 * tests can supply an answerer and a seam without a running broker.
 */
export interface RegisterDeps {
  readonly answer?: AnswerDeps
  readonly makeSeam?: SeamFactory
  readonly getMessagingHost?: LazyMessagingHost
}

export function register(ctx: HostContext, config: HostConfig = {}, deps: RegisterDeps = {}): void {
  const answer = deps.answer ?? defaultAnswer
  const makeSeam = deps.makeSeam ?? defaultSeamFactory
  const messaging = config.messaging === true
  const parentRunId = config.parentRunId ?? HOST_RUN_ID

  ctx.subagents.registerProvider(
    createAvenorProvider(makeSeam({ selection: selectionOf(config), messaging, parentRunId })),
  )
  ctx.tools.register(answerToolDefinition(answer))

  const getHost = deps.getMessagingHost ?? createLazyMessagingHost()
  ctx.tools.register(messageToolDefinition(getHost))
  ctx.tools.register(peersToolDefinition(getHost))
}

/**
 * Cordis entry point. `config` is the patch row's `config` object, so a profile
 * chooses the delegated child's route in YAML rather than in code.
 */
export function apply(ctx: HostContext, config: HostConfig = {}): void {
  register(ctx, config)
}

import { answerPermissionTool } from '@dougbots/avenor-core'
import { answerToolDefinition, type AnswerDeps } from './answer-tool.js'
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

/**
 * Register the provider and the answer tool. Kept separate from `apply` so
 * tests can supply an answerer and a seam without a running broker.
 */
export function register(
  ctx: HostContext,
  config: HostConfig = {},
  answer: AnswerDeps = defaultAnswer,
  makeSeam: SeamFactory = defaultSeamFactory,
): void {
  ctx.subagents.registerProvider(createAvenorProvider(makeSeam(selectionOf(config))))
  ctx.tools.register(answerToolDefinition(answer))
}

/**
 * Cordis entry point. `config` is the patch row's `config` object, so a profile
 * chooses the delegated child's route in YAML rather than in code.
 */
export function apply(ctx: HostContext, config: HostConfig = {}): void {
  register(ctx, config)
}

import { answerPermissionTool } from '@dougbots/avenor-core'
import { answerToolDefinition, type AnswerDeps } from './answer-tool.js'
import { createAvenorProvider, createCoreSeam } from './provider.js'
import type { HostContext } from './types.js'

/** Wait for both services this plugin registers into. */
export const inject = ['subagents', 'tools']

const defaultAnswer: AnswerDeps = {
  answer: (request) => answerPermissionTool(request),
}

/**
 * Register the provider and the answer tool. Kept separate from `apply` so
 * tests can supply an answerer without a running broker.
 */
export function register(ctx: HostContext, answer: AnswerDeps = defaultAnswer): void {
  ctx.subagents.registerProvider(createAvenorProvider(createCoreSeam()))
  ctx.tools.register(answerToolDefinition(answer))
}

export function apply(ctx: HostContext): void {
  register(ctx)
}

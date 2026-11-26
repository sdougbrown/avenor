import { createAvenorProvider, createCoreSeam } from './provider.js'
import type { HostContext } from './types.js'

/** Wait for the subagent registry before activating. */
export const inject = ['subagents']

export function apply(ctx: HostContext): void {
  ctx.subagents.registerProvider(createAvenorProvider(createCoreSeam()))
}

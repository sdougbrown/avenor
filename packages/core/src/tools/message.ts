import { executeMessage } from '../message.js'
import type { MessageProvider, ProviderResult } from '../message.js'

export { ToolError } from '../message.js'

/*
 * The port lives with the router that consumes it (packages/core/src/message.ts);
 * this module is the handler every surface binds, so it re-exports the port for
 * the callers that import it from here.
 */
export type {
  MessageCallerKind,
  MessageInput,
  MessageSurface,
  MessageProvider,
  PeerRecord,
  PeersProviderResult,
  ProviderResult,
  Resolution,
  RunFacts,
} from '../message.js'

/**
 * The one avenor_message handler: every surface routes through it, so the six
 * actions and their failure text exist once. A router tool error is thrown as a
 * typed Error, which each surface reports as its own error result.
 */
export async function messageTool(
  args: unknown,
  provider: MessageProvider,
  options?: { signal?: AbortSignal },
): Promise<ProviderResult> {
  return executeMessage(provider, args, options)
}

/** Bind the handler to one provider. */
export function createMessageTool(
  provider: MessageProvider,
): (args: unknown, options?: { signal?: AbortSignal }) => Promise<ProviderResult> {
  return (args, options) => messageTool(args, provider, options)
}

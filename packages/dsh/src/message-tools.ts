// Copied from the repo's schemas/*.flat.json, which stay the single source for
// every surface; a test fails if the copy drifts.
import messageSchema from './schemas/message.input.flat.json'
import peersSchema from './schemas/peers.input.flat.json'
import type { ToolDefinitionLike, ToolExecContext } from './answer-tool.js'
import type { MessagingSurface } from './messaging.js'

export const MESSAGE_TOOL_NAME = 'avenor_message'
export const PEERS_TOOL_NAME = 'avenor_peers'

/** The caller's DSH session; the bound conversation is selected by it. */
export function sessionIdOf(exec: ToolExecContext | undefined): string | undefined {
  return exec?.agent?.session?.id
}

// A raw JSON Schema the registry accepts: `{ type: 'json' }` is the authoring
// DSL's shorthand, but `tools.register` asserts the raw subset, where the only
// types are object/array/string/number/integer/boolean/null. Getting this wrong
// throws at registration and rolls the whole plugin back, tools included.
const outputSchema = { type: 'object', properties: {}, additionalProperties: true }

function renderValue(value: unknown) {
  const record = (value ?? {}) as { text?: unknown; outcome?: unknown }
  const text =
    typeof record.text === 'string' && record.text !== ''
      ? record.text
      : typeof record.outcome === 'string'
        ? `message ${record.outcome}`
        : JSON.stringify(value)
  return [{ type: 'text' as const, text }]
}

/**
 * The message tool's parameters are the flat schema the MCP surfaces publish,
 * not a hand-written copy: one field list for every surface, so a renamed field
 * cannot drift between them.
 */
export function messageToolDefinition(surface: MessagingSurface): ToolDefinitionLike {
  return {
    name: MESSAGE_TOOL_NAME,
    description:
      'Send and answer messages between Avenor runs: tell, ask, reply, resume, withdraw, or ' +
      'answer a control message. Targets resolve against the peer listing; a delegated child ' +
      'that was spawned with messaging enabled can address this harness as "supervisor".',
    parameters: messageSchema as unknown as ToolDefinitionLike['parameters'],
    output: { schema: outputSchema, render: (_args, value) => renderValue(value) },
    execute: async (args, exec) => surface.message(args, sessionIdOf(exec)),
  }
}

export function peersToolDefinition(surface: MessagingSurface): ToolDefinitionLike {
  return {
    name: PEERS_TOOL_NAME,
    description:
      'List connected Avenor runs, rename a run label, or read a peer\'s last agent message.',
    parameters: peersSchema as unknown as ToolDefinitionLike['parameters'],
    output: { schema: outputSchema, render: (_args, value) => renderValue(value) },
    execute: async (args, exec) => surface.peers(args, sessionIdOf(exec)),
  }
}

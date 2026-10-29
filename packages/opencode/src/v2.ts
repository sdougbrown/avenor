import type { Plugin } from '@opencode/plugin'
import { tool } from '@opencode-ai/plugin'

import { createAvenorCore, type AvenorCore, type PermissionAsk, type PluginHost } from './core.js'

type V2Context = Plugin.Context

type ToolResultLike = {
  title?: string
  output: string
  metadata?: Record<string, unknown>
  attachments?: Array<{ type: string; mime: string; url: string; filename?: string }>
}

/**
 * Project the V1 tool result shape onto the V2 Tool.Result shape. A title has
 * no V2 field, so it rides in metadata; V1 attachments map onto V2 file
 * content entries.
 */
export function toV2ToolResult(result: string | ToolResultLike): {
  content: string | Array<{ type: 'text'; text: string } | { type: 'file'; uri: string; mime: string; name?: string }>
  metadata?: Record<string, unknown>
} {
  if (typeof result === 'string') return { content: result }
  const content: Array<{ type: 'text'; text: string } | { type: 'file'; uri: string; mime: string; name?: string }> = [
    { type: 'text', text: result.output },
    ...(result.attachments ?? []).map(attachment => ({
      type: 'file' as const,
      uri: attachment.url,
      mime: attachment.mime,
      ...(attachment.filename ? { name: attachment.filename } : {}),
    })),
  ]
  return {
    content,
    metadata: {
      ...(result.metadata ?? {}),
      ...(result.title ? { title: result.title } : {}),
    },
  }
}

/** The host adapter onto the V2 session domain. */
export function v2Host(ctx: V2Context): PluginHost {
  return {
    promptAsync: (sessionId, text) =>
      ctx.session.prompt({ sessionID: sessionId, text, delivery: 'steer' })
        .then(() => undefined)
        .catch(() => undefined),
    getMessage: async (sessionId, messageId) => {
      // The V2 session domain exposes no single-message read; the message is
      // looked up in the session context instead.
      const context = await ctx.session.context({ sessionID: sessionId })
      const message = context.find((entry) => entry.id === messageId)
      return message as { parts?: unknown[]; content?: unknown[] } | undefined
    },
  }
}

type PermissionEventLike = {
  sessionID: string
  action: string
  resources: ReadonlyArray<string>
  source?: { type: string; id: string }
}

/**
 * Map the V2 permission evaluate event onto the V1 permission-ask shape. The
 * source id is a tool-call ID, not the permission record ID the answer tool
 * matches on, so it is deliberately not forwarded; the answer tool's
 * auto-discovery resolves the real request ID.
 */
export function permissionAskFromEvent(event: PermissionEventLike): PermissionAsk {
  return {
    sessionID: event.sessionID,
    type: event.action,
    pattern: [...event.resources],
    id: undefined,
  }
}

/** The ask-only evaluate hook: an ask routes to the core, any other effect is left to the rules. */
export function evaluatePermissionAsk(core: Pick<AvenorCore, 'onPermissionAsk'>) {
  return async (event: PermissionEventLike & { effect: string }): Promise<void> => {
    if (event.effect !== 'ask') return
    await core.onPermissionAsk(permissionAskFromEvent(event))
  }
}

/** One V1 tool-definition entry, typed loosely so both generations can register it. */
type ToolDefinitionLike = {
  description: string
  args: Record<string, unknown>
  execute(input: Record<string, unknown>, context: Record<string, unknown>): Promise<unknown>
}

type ToolContextLike = {
  sessionID: string
  agent: string
  messageID: string
  signal: AbortSignal
  progress(update: Record<string, unknown>): Promise<void>
}

/**
 * One registered V2 tool. The execute wrapper builds the V1 tool context —
 * the session's own directory when readable, the plugin location otherwise —
 * and projects the result onto the V2 shape. The per-session directory is
 * cached after its first read; a session's location does not move. The cache
 * is shared across every tool of one plugin instance.
 */
export function v2ToolEntry(
  name: string,
  definition: ToolDefinitionLike,
  ctx: Pick<V2Context, 'location' | 'session'>,
  directories: Map<string, string> = new Map(),
): { name: string; description: string; input: unknown; execute: (input: unknown, context: ToolContextLike) => Promise<unknown> } {
  return {
    name,
    description: definition.description,
    input: tool.schema.toJSONSchema(tool.schema.object(definition.args as never)),
    execute: async (rawInput, context) => {
      let directory = directories.get(context.sessionID)
      if (directory === undefined) {
        // Only a successful lookup is cached; a failed one falls back for this
        // call and retries next time.
        const session = await ctx.session.get({ sessionID: context.sessionID }).catch(() => undefined)
        if (session) {
          directory = session?.location?.directory
          if (directory) directories.set(context.sessionID, directory)
        }
        directory ??= ctx.location.directory
      }
      const result = await definition.execute(rawInput as Record<string, unknown>, {
        sessionID: context.sessionID,
        agent: context.agent,
        messageID: context.messageID,
        directory,
        worktree: directory,
        abort: context.signal,
        metadata: (update: Record<string, unknown>) => {
          void context.progress(update)
        },
        ask: () => {
          throw new Error('permission prompts are not supported in OpenCode 2 tool context')
        },
      })
      return toV2ToolResult(result as string | ToolResultLike)
    },
  }
}

type V2Event = { type: string; data?: unknown }

/**
 * One subscription event. The try/catch is per event, so a failed handler
 * cannot end the subscription: the next session event still has to re-prompt
 * monitors and publish turns.
 */
export function createV2EventHandler(core: Pick<AvenorCore, 'onSessionIdle' | 'onStepEnded'>) {
  return async (event: V2Event): Promise<void> => {
    try {
      if (event.type === 'session.idle') {
        const sessionID = (event.data as { sessionID?: string } | undefined)?.sessionID
        if (sessionID) await core.onSessionIdle(sessionID)
        return
      }
      if (event.type === 'session.step.ended') {
        const data = event.data as { sessionID?: string; assistantMessageID?: string } | undefined
        if (data?.sessionID && data?.assistantMessageID) {
          await core.onStepEnded(data.sessionID, data.assistantMessageID)
        }
      }
    } catch (error) {
      console.error('[avenor] event handling failed:', error instanceof Error ? error.message : error)
    }
  }
}

/**
 * The OpenCode 2 plugin setup. The shared core in core.ts owns the behavior;
 * this adapter registers it through the V2 domains: tools become a transform,
 * the event hook a subscription, permission.ask an evaluate hook, and
 * chat.message the prompt hook. V1's experimental.text.complete has no V2
 * equivalent, so generated channel blocks are only reformatted on admission.
 */
export async function setupAvenor(ctx: V2Context): Promise<() => void> {
  const core = createAvenorCore(v2Host(ctx))

  await ctx.tool.transform((editor) => {
    const directories = new Map<string, string>()
    for (const [name, definition] of Object.entries(core.tool ?? {})) {
      editor.add(v2ToolEntry(name, definition, ctx, directories) as never)
    }
  })

  const controller = new AbortController()
  const onEvent = createV2EventHandler(core)
  void (async () => {
    for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
      await onEvent(event as V2Event)
    }
  })().catch(console.error)

  await ctx.permission.hook('evaluate', evaluatePermissionAsk(core))

  await ctx.session.hook('prompt', (event) => {
    if (event.prompt.text.includes('<channel ')) {
      event.prompt.text = core.formatChannelBlocks(event.prompt.text)
    }
  })

  return () => controller.abort()
}

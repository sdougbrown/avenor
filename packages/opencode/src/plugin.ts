import type { Plugin } from '@opencode-ai/plugin'

import { createAvenorCore } from './core.js'

export { buildWaitingText, hostIdentityMetadata, terminalStatus } from './core.js'
export { createAvenorCore, type PluginHost } from './core.js'

/**
 * The refusal a prompt result carries. The generated client resolves the 4xx
 * body unless it was built with throwOnError, so the caller sees the failure in
 * either shape and hands the core one.
 */
function promptRefusalError(error: unknown): Error {
  if (error instanceof Error) return error
  const body = error && typeof error === 'object'
    ? error as { data?: unknown; message?: unknown; name?: unknown }
    : undefined
  const data = body?.data
  const message = data && typeof data === 'object' && typeof (data as { message?: unknown }).message === 'string'
    ? (data as { message: string }).message
    : typeof body?.message === 'string' ? body.message
    : typeof body?.name === 'string' ? body.name
    : undefined
  return new Error(message || 'the host refused the prompt')
}

/** The V1 plugin entrypoint. OpenCode 2 uses the default export in index.ts. */
export const AvenorPlugin: Plugin = async (ctx) => {
  const core = createAvenorCore({
    promptAsync: async (sessionId, text) => {
      // A refused prompt must reach the core: its delivery path owns the feed,
      // and a swallowed rejection leaves that feed draining unattended.
      const result = await ctx.client.session.promptAsync({
        path: { id: sessionId },
        body: { parts: [{ type: 'text', text }] },
      })
      if (result && result.error) throw promptRefusalError(result.error)
    },
    getMessage: async (sessionId, messageId) => {
      const response = await ctx.client.session.message({ path: { id: sessionId, messageID: messageId } })
      return response.data
    },
    getSessionTitle: async (sessionId) => {
      const response = await ctx.client.session.get({ path: { id: sessionId } }).catch(() => undefined)
      const session = response?.data as { title?: unknown } | undefined
      return typeof session?.title === 'string' ? session.title : undefined
    },
  })
  return {
    event: core.event,
    'permission.ask': core['permission.ask'],
    'chat.message': core['chat.message'],
    'experimental.text.complete': core['experimental.text.complete'],
    tool: core.tool,
  }
}

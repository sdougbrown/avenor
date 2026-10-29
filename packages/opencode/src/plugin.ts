import type { Plugin } from '@opencode-ai/plugin'

import { createAvenorCore } from './core.js'

export { buildWaitingText, hostIdentityMetadata, terminalStatus } from './core.js'
export { createAvenorCore, type PluginHost } from './core.js'

/** The V1 plugin entrypoint. OpenCode 2 uses the default export in index.ts. */
export const AvenorPlugin: Plugin = async (ctx) => {
  const core = createAvenorCore({
    promptAsync: async (sessionId, text) => {
      await ctx.client.session.promptAsync({
        path: { id: sessionId },
        body: { parts: [{ type: 'text', text }] },
      }).catch(() => {})
    },
    getMessage: async (sessionId, messageId) => {
      const response = await ctx.client.session.message({ path: { id: sessionId, messageID: messageId } })
      return response.data
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

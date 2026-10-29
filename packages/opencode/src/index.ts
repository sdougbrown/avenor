import { Plugin } from '@opencode/plugin'

import { AvenorPlugin } from './plugin.js'
import { setupAvenor } from './v2.js'

export { AvenorPlugin } from './plugin.js'
export { createAvenorCore, formatChannelBlocks, terminalStatus, hostIdentityMetadata, buildWaitingText, type PluginHost } from './core.js'
export { setupAvenor, toV2ToolResult } from './v2.js'

/**
 * One entrypoint for both OpenCode generations: V2 reads `id` and `setup()`,
 * V1 (1.18.29+) calls `server()`.
 */
export default {
  ...Plugin.define({
    id: 'avenor',
    setup: setupAvenor,
  }),
  async server(ctx: Parameters<typeof AvenorPlugin>[0]) {
    return AvenorPlugin(ctx)
  },
}

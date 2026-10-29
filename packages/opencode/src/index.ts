import { AvenorPlugin } from './plugin.js'
import { setupAvenor } from './v2.js'

export { AvenorPlugin } from './plugin.js'
export { createAvenorCore, formatChannelBlocks, terminalStatus, hostIdentityMetadata, buildSnapshotMetadata, buildWaitingText, type PluginHost } from './core.js'
export { setupAvenor, toV2ToolResult, v2Host, v2ToolEntry, createV2EventHandler, permissionAskFromEvent, evaluatePermissionAsk } from './v2.js'
export { stepEndedInfo } from './core.js'

/**
 * One entrypoint for both OpenCode generations. The exported shape is what
 * each loader reads — V2 reads `id` and `setup()`, V1 (1.18.29+) calls
 * `server()` — and no OpenCode 2 package is required at runtime, so an
 * OpenCode 1 host needs only the V1 plugin package.
 */
export default {
  id: 'avenor',
  setup: setupAvenor,
  async server(ctx: Parameters<typeof AvenorPlugin>[0]) {
    return AvenorPlugin(ctx)
  },
}

import * as core from '@dougbots/avenor-core'

// This facade owns its values: locally-declared consts, not `export *`
// re-exports. bun's module mocks rewrite re-export binding slots back onto the
// shared specifier (the slots alias the same storage), so a mock on an
// `export *` facade leaks into every other package's view of
// '@dougbots/avenor-core'. Owned consts break that aliasing: mocking this
// module rebinds only the facade's own slots. See mcp.test.ts for the mock.
export const {
  spawnTool,
  statusTool,
  resultTool,
  answerPermissionTool,
  eventsTool,
  killTool,
  shutdownTool,
  createHostMessageProvider,
  getSupervisorClient,
  isGenerationMismatchError,
  messageTool,
  peersDetails,
  peersTool,
  renderPeerList,
  resultOutput,
  ToolError,
  runWorkflowTool,
  validateSpawnSelection,
} = core

export type {
  MessageProvider,
  PeersRouterResult,
} from '@dougbots/avenor-core'

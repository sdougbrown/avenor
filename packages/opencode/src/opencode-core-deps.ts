import * as core from '@dougbots/avenor-core'

// This facade owns its values: locally-declared consts, not `export *`
// re-exports. bun's module mocks rewrite re-export binding slots back onto the
// shared specifier (the slots alias the same storage), so a mock on an
// `export *` facade leaks into every other package's view of
// '@dougbots/avenor-core'. Owned consts break that aliasing: mocking this
// module rebinds only the facade's own slots. See plugin.test.ts for the mock.
export const {
  answerPermissionTool,
  boundFinalOutput,
  createHostMessageProvider,
  createRunSnapshot,
  dial,
  eventsTool,
  extractTurnText,
  getSupervisorClient,
  inspectTool,
  isClosedSocketError,
  messageTool,
  normalizeTurnStopReason,
  observeRun,
  peersTool,
  resultTool,
  shutdownTool,
  killTool,
  spawnTool,
  statusTool,
  Supervisor,
  THINKING_LEVELS,
  validateSpawnSelection,
  runWorkflowTool,
} = core

export type {
  MessageProvider,
  ProviderResult,
  RunObserver,
  RunSnapshot,
  RunSnapshotView,
  StatusResult,
} from '@dougbots/avenor-core'

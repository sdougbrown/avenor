export { dial } from './client.js'
export type {
  Client,
  Event,
  HistoryOptions,
  HistoryResult,
  SpawnParams,
  SubscribeOptions,
} from './client.js'
export { normalizeRunEvent, extractEventText } from './run-events.js'
export type {
  NormalizedRunEvent,
  RunChunkRole,
  RunPermissionOption,
  RunPermissionSummary,
  RunToolSummary,
} from './run-events.js'
export {
  createRunSnapshot,
  DEFAULT_RUN_REDUCER_LIMITS,
  reduceRunSnapshot,
  RunReducer,
} from './run-reducer.js'
export type {
  RunIdentity,
  RunReducerLimits,
  RunSnapshot,
  RunSnapshotSeed,
  RunToolState,
  RunTranscriptEntry,
} from './run-reducer.js'
export { observeRun } from './run-observer.js'
export type { ObserveRunOptions, RunObserver, RunObserverClient } from './run-observer.js'
export { Supervisor, findAvenorBinary } from './supervisor.js'
export { getInstallDir, getVersion, installerBinaryPath } from './install-path.js'
export type { RunInfo, SupervisorOptions } from './supervisor.js'

export { spawnTool } from './tools/spawn.js'
export { statusTool } from './tools/status.js'
export type { StatusResult } from './tools/status.js'
export { answerPermissionTool } from './tools/answer-permission.js'
export { followUpTool } from './tools/follow-up.js'
export { eventsTool } from './tools/events.js'
export { inspectTool } from './tools/inspect.js'
export type { InspectResult } from './tools/inspect.js'
export { shutdownTool } from './tools/shutdown.js'

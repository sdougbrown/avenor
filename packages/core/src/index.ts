export { dial } from './client.js'
export type {
  Client,
  Event,
  HistoryOptions,
  HistoryResult,
  SpawnParams,
  SpawnResult,
  SubscribeOptions,
  ThinkingLevel,
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
export { socketsRoot } from './paths.js'
export { getInstallDir, getVersion, installerBinaryPath } from './install-path.js'
export type { RunInfo, SpawnMetadata, SupervisorOptions } from './supervisor.js'

export { spawnTool } from './tools/spawn.js'
export type { SpawnToolArgs, SpawnToolResult } from './tools/spawn.js'
export { validateSpawnSelection } from './spawn-selection.js'
export type { SpawnSelectionInput } from './spawn-selection.js'
export {
  THINKING_LEVELS,
  evaluateThinkingPolicy,
  isThinkingLevel,
  validateThinking,
  validateThinkingForBackend,
  validateThinkingForBackendResume,
} from './thinking-policy.js'
export type { ThinkingOutcome } from './thinking-policy.js'
export { statusTool } from './tools/status.js'
export type { StatusResult, StatusToolArgs, StatusView } from './tools/status.js'
export { resultTool } from './tools/result.js'
export type { ResultResult, ResultToolArgs } from './tools/result.js'
export { answerPermissionTool } from './tools/answer-permission.js'
export { followUpTool } from './tools/follow-up.js'
export type { FollowUpToolArgs, FollowUpToolResult } from './tools/follow-up.js'
export { eventsTool } from './tools/events.js'
export { inspectTool } from './tools/inspect.js'
export type { InspectResult } from './tools/inspect.js'
export { shutdownTool } from './tools/shutdown.js'

export type { ExecutionIdentity, WorkflowGateOperation, WorkflowCompleteParams, WorkflowGateParams, WorkflowHeartbeatParams } from './client.js'
export { workflowStatusTool, createWorkflowStatusTool, type WorkflowStatusToolArgs, type WorkflowStatusResult } from './tools/workflow-status.js'
export { workflowWaitTool, createWorkflowWaitTool, type WorkflowWaitToolArgs, type WorkflowWaitResult } from './tools/workflow-wait.js'
export { workflowInspectTool, createWorkflowInspectTool, type WorkflowInspectToolArgs, type WorkflowInspectResult } from './tools/workflow-inspect.js'
export { workflowEventsTool, createWorkflowEventsTool, type WorkflowEventsToolArgs, type WorkflowEventsResult } from './tools/workflow-events.js'
export { workflowCompleteTool, createWorkflowCompleteTool, type WorkflowCompleteToolArgs, type WorkflowCompleteResult } from './tools/workflow-complete.js'
export { workflowGateTool, createWorkflowGateTool, type WorkflowGateToolArgs, type WorkflowGateResult } from './tools/workflow-gate.js'

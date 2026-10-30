export { dial, RpcError, OwnerAuthError, ClosedSocketError, isClosedSocketError } from './client.js'
export {
  attachOwner,
  ForeignPeerError,
  OwnerRecordMissingError,
  StaleOwnerRecordError,
} from './owner-attach.js'
export type { OwnerAttachOptions } from './owner-attach.js'
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
  RunSnapshotView,
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
export type { StatusResult, StatusListResult, StatusToolArgs, StatusView } from './tools/status.js'
export { resultTool } from './tools/result.js'
export type { ResultResult, ResultToolArgs } from './tools/result.js'
export { answerPermissionTool } from './tools/answer-permission.js'
export { eventsTool } from './tools/events.js'
export { inspectTool } from './tools/inspect.js'
export type { InspectResult } from './tools/inspect.js'
export { shutdownTool } from './tools/shutdown.js'

export {
  extractReplyMessage,
  renderAskFailure,
  normalizeAskTimeoutMs,
  renderOutcome,
  repair,
  renderLast,
  quote,
  sanitize,
  quoteList,
  newMsgID,
  MAX_WAIT_MS,
  MAX_FINAL_OUTPUT_RUNES,
  MESSAGE_OUTCOMES,
  normalizeTurnStopReason,
  extractTurnText,
  boundFinalOutput,
} from './messaging-text.js'
export type { MessageOutcome } from './messaging-text.js'
export {
  ToolError,
  assertMessageInput,
  assertPeersInput,
  executeMessage,
  executePeers,
  peersDetails,
  renderPeerList,
  resultOutput,
} from './message.js'
export type { MessageInput, PeersInput, PeersRouterResult } from './message.js'
export { messageTool, createMessageTool } from './tools/message.js'
export type {
  MessageProvider,
  MessageCallerKind,
  MessageSurface,
  ProviderResult,
  Resolution,
  RunFacts,
  PeerRecord,
  PeersProviderResult,
} from './tools/message.js'
export { createHostMessageProvider, createChildMessageProvider, installAskClaims, isGenerationMismatchError, matchPeerRows, SUPERVISOR_RUN_ID } from './tools/message-provider.js'
export type { AskClaims, ChildBroker, MessageClient, PendingAsk } from './tools/message-provider.js'
export { peersTool, createPeersTool } from './tools/peers.js'
export type { PeersResult, PeersToolArgs } from './tools/peers.js'
export { getSupervisorClient } from './tools/get-supervisor-client.js'
export type { SupervisorClientOptions } from './tools/get-supervisor-client.js'
export { killTool } from './tools/kill.js'
export type { KillToolArgs, KillResult } from './tools/kill.js'
export { brokerReceiveTool } from './tools/broker-receive.js'
export type { BrokerReceiveArgs, InboundAsk, BrokerReceiveResult } from './tools/broker-receive.js'

export type { ExecutionIdentity, WorkflowGateOperation, WorkflowCompleteParams, WorkflowGateParams, WorkflowHeartbeatParams, WorkflowControllerCreateParams } from './client.js'
export { WorkflowToolError } from './workflow.js'
export type { WorkflowAction, WorkflowInput, WorkflowProvider, WorkflowResult } from './workflow.js'
export { runWorkflowTool } from './tools/workflow.js'


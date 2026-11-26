/**
 * The subset of the DeepSeek Harness subagent contract this spike needs.
 *
 * Declared locally on purpose: a bundle plugin resolves DSH's own packages
 * from the installed Harness, so it must not depend on them. Field names and
 * the stop-reason vocabulary mirror
 * `@deepseek-ai/dsh-subagent/lib/types/types.d.ts`. When the provider is
 * promoted past the spike, import the real types and delete this file — the
 * shapes are structural, so that swap is mechanical.
 */

export type ContentBlock =
  | { readonly type: 'text'; readonly text: string }
  | { readonly type: string; readonly [key: string]: unknown }

export type SubagentStopReason = 'completed' | 'aborted' | 'error' | 'max-tokens' | 'refusal'

/** A user-role message a plugin can inject into an agent's inbox. */
export interface UserMessageLike {
  readonly id: string
  readonly role: 'user'
  readonly content: readonly ContentBlock[]
  readonly source: { readonly kind: 'user' }
}

/**
 * The agent that started the delegation. It owns the permission decision:
 * `inject` reaches it mid-turn, `followup` starts a turn when it is idle.
 */
export interface ParentAgent {
  readonly status: 'idle' | 'running'
  /** The DSH session this agent speaks for; a conversation binds by its id. */
  readonly session?: { readonly id: string }
  inject(message: UserMessageLike): void
  followup(message: UserMessageLike): void
}

export interface SubagentStartRequest {
  readonly prompt: readonly ContentBlock[]
  readonly cwd?: string
  readonly label?: string
  /** Cancellation signal from the spawning tool call. */
  readonly signal: AbortSignal
  /** The spawning agent; the child's permission decisions go to it. */
  readonly parent: ParentAgent
}

export interface SubagentResult {
  readonly output: readonly ContentBlock[]
  readonly diagnostic?: string
  readonly stopReason: SubagentStopReason
}

export interface SubagentRun {
  readonly id: string
  readonly result: Promise<SubagentResult>
  dispose(): Promise<void>
}

export interface SubagentCapabilities {
  readonly agentOptions: boolean
  readonly agentPreset: boolean
  readonly outputSchema: boolean
  readonly depthLimit: boolean
}

export interface SubagentProvider {
  readonly name: string
  readonly capabilities: SubagentCapabilities
  readonly inheritsParentContext: boolean
  start?(request: SubagentStartRequest): Promise<SubagentRun>
}

/** Matches `NO_START_CAPABILITIES` in `@deepseek-ai/dsh-subagent`. */
export const NO_START_CAPABILITIES: SubagentCapabilities = Object.freeze({
  agentOptions: false,
  agentPreset: false,
  outputSchema: false,
  depthLimit: false,
})

/** The Host services this plugin waits for before it activates. */
export interface HostContext {
  subagents: {
    registerProvider(provider: SubagentProvider): () => void
  }
  tools: {
    register(definition: unknown): () => void
  }
  /** Cordis effect scope: the returned disposer runs when the plugin unloads. */
  effect?(scope: () => (() => void) | void): () => void
  /** Per-session working directory; names a session's conversation. */
  workingDirectory?: {
    get(session: unknown): string
  }
}

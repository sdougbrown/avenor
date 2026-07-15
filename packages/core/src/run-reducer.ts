import {
  normalizeRunEvent,
  type NormalizedRunEvent,
  type RunPermissionSummary,
  type RunToolSummary,
} from './run-events.js'

export interface RunReducerLimits {
  transcript_entries: number
  text_chars: number
  tool_preview_chars: number
  tools: number
  permissions: number
  legacy_keys: number
}

export const DEFAULT_RUN_REDUCER_LIMITS: RunReducerLimits = {
  transcript_entries: 64,
  text_chars: 4_000,
  tool_preview_chars: 1_000,
  tools: 24,
  permissions: 16,
  legacy_keys: 128,
}

export interface RunTranscriptEntry {
  kind: 'assistant' | 'thought' | 'user' | 'tool' | 'permission' | 'session' | 'status'
  event: string
  source_event: string
  text?: string
  title?: string
  status?: string
  request_id?: string
  seq?: number
  ts?: number
}

export interface RunToolState extends RunToolSummary {
  started_at?: number
  updated_at?: number
  ended_at?: number
  live: boolean
}

export interface RunIdentity {
  runtime_id?: string
  session_id?: string
  run_id?: string
  run_label?: string
  backend?: string
  agent?: string
  model?: string
  dir?: string
  parent_id?: string
  children?: string[]
  event_path?: string
}

export interface RunSnapshot {
  identity: RunIdentity
  limits: RunReducerLimits
  latest_seq?: number
  last_event?: string
  last_ts?: number
  phase?: string
  phase_label?: string
  ended: boolean
  stop_reason?: string
  usage?: Record<string, unknown>
  final_output?: string
  assistant_text: string
  thought_text: string
  user_text: string
  transcript: RunTranscriptEntry[]
  tools: RunToolState[]
  live_tools: RunToolState[]
  pending_permission?: RunPermissionSummary
  permissions: RunPermissionSummary[]
  _seen_seq_by_scope: Record<string, number>
  _recent_legacy_keys: string[]
  _last_semantic_fingerprint?: string
  _last_source_event?: string
}

function clipText(text: string | undefined, limit: number): string | undefined {
  if (!text) return undefined
  if (text.length <= limit) return text
  return text.slice(text.length - limit)
}

function appendText(current: string, delta: string | undefined, limit: number): string {
  if (!delta) return current
  return clipText(`${current}${delta}`, limit) ?? current
}

function pushBounded<T>(items: T[], item: T, limit: number): T[] {
  const next = [...items, item]
  if (next.length <= limit) return next
  return next.slice(next.length - limit)
}

function toLegacyKey(event: NormalizedRunEvent): string {
  return JSON.stringify([
    event.runtime_id ?? event.session_id ?? '',
    event.event,
    event.ts ?? '',
    event.delta ?? '',
    event.tool?.id ?? event.tool?.title ?? event.tool?.status ?? '',
    event.permission?.request_id ?? event.permission?.question ?? '',
    event.stop_reason ?? '',
  ])
}

function normalizeLimits(limits?: Partial<RunReducerLimits>): RunReducerLimits {
  return {
    transcript_entries: Math.max(1, Math.trunc(limits?.transcript_entries ?? DEFAULT_RUN_REDUCER_LIMITS.transcript_entries)),
    text_chars: Math.max(1, Math.trunc(limits?.text_chars ?? DEFAULT_RUN_REDUCER_LIMITS.text_chars)),
    tool_preview_chars: Math.max(1, Math.trunc(limits?.tool_preview_chars ?? DEFAULT_RUN_REDUCER_LIMITS.tool_preview_chars)),
    tools: Math.max(1, Math.trunc(limits?.tools ?? DEFAULT_RUN_REDUCER_LIMITS.tools)),
    permissions: Math.max(1, Math.trunc(limits?.permissions ?? DEFAULT_RUN_REDUCER_LIMITS.permissions)),
    legacy_keys: Math.max(1, Math.trunc(limits?.legacy_keys ?? DEFAULT_RUN_REDUCER_LIMITS.legacy_keys)),
  }
}

export function createRunSnapshot(limits?: Partial<RunReducerLimits>): RunSnapshot {
  return {
    identity: {},
    limits: normalizeLimits(limits),
    ended: false,
    assistant_text: '',
    thought_text: '',
    user_text: '',
    transcript: [],
    tools: [],
    live_tools: [],
    permissions: [],
    _seen_seq_by_scope: {},
    _recent_legacy_keys: [],
  }
}

function transcriptText(entry: RunTranscriptEntry, limits: RunReducerLimits): RunTranscriptEntry {
  if (!entry.text) return entry
  return { ...entry, text: clipText(entry.text, limits.text_chars) }
}

function appendTranscript(snapshot: RunSnapshot, entry: RunTranscriptEntry): RunTranscriptEntry[] {
  const nextEntry = transcriptText(entry, snapshot.limits)
  const last = snapshot.transcript[snapshot.transcript.length - 1]
  if (
    last &&
    last.kind === nextEntry.kind &&
    nextEntry.text &&
    last.text &&
    last.event === nextEntry.event
  ) {
    const merged = {
      ...last,
      text: clipText(`${last.text}${nextEntry.text}`, snapshot.limits.text_chars),
      seq: nextEntry.seq ?? last.seq,
      ts: nextEntry.ts ?? last.ts,
    }
    return [...snapshot.transcript.slice(0, -1), merged]
  }
  return pushBounded(snapshot.transcript, nextEntry, snapshot.limits.transcript_entries)
}

function mergeIdentity(snapshot: RunSnapshot, event: NormalizedRunEvent): RunIdentity {
  return {
    runtime_id: event.runtime_id ?? snapshot.identity.runtime_id,
    session_id: event.session_id ?? snapshot.identity.session_id,
    run_id: event.run_id ?? snapshot.identity.run_id,
    run_label: event.run_label ?? snapshot.identity.run_label,
    backend: event.backend ?? snapshot.identity.backend,
    agent: event.agent ?? snapshot.identity.agent,
    model: event.model ?? snapshot.identity.model,
    dir: event.dir ?? snapshot.identity.dir,
    parent_id: event.parent_id ?? snapshot.identity.parent_id,
    children: event.children ?? snapshot.identity.children,
    event_path: event.event_path ?? snapshot.identity.event_path,
  }
}

function withToolPreview(tool: RunToolSummary | undefined, limit: number): RunToolSummary | undefined {
  if (!tool) return tool
  return {
    ...tool,
    preview: clipText(tool.preview, limit),
  }
}

function mergeToolState(snapshot: RunSnapshot, event: NormalizedRunEvent, live: boolean): RunToolState[] {
  const tool = withToolPreview(event.tool, snapshot.limits.tool_preview_chars)
  if (!tool) return snapshot.tools

  const tools = snapshot.tools.map((item) => ({ ...item }))
  const index = tools.findIndex((item) => item.key === tool.key)
  const existing = index >= 0 ? tools[index] : undefined
  const next: RunToolState = {
    key: tool.key,
    id: tool.id ?? existing?.id,
    title: tool.title ?? existing?.title,
    kind: tool.kind ?? existing?.kind,
    status: tool.status ?? existing?.status,
    preview: clipText(
      tool.preview ?? existing?.preview,
      snapshot.limits.tool_preview_chars,
    ),
    started_at: existing?.started_at ?? event.ts,
    updated_at: event.ts ?? existing?.updated_at,
    ended_at: live ? existing?.ended_at : (event.ts ?? existing?.ended_at),
    live,
  }

  if (index >= 0) {
    tools[index] = next
  } else {
    tools.push(next)
  }

  const bounded = tools.length <= snapshot.limits.tools
    ? tools
    : tools.slice(tools.length - snapshot.limits.tools)
  return bounded
}

function liveTools(tools: RunToolState[]): RunToolState[] {
  return tools.filter((tool) => tool.live)
}

function mergePermissionHistory(snapshot: RunSnapshot, permission: RunPermissionSummary | undefined, resolved = false): {
  pending_permission: RunPermissionSummary | undefined
  permissions: RunPermissionSummary[]
} {
  if (!permission) {
    return {
      pending_permission: resolved ? undefined : snapshot.pending_permission,
      permissions: snapshot.permissions,
    }
  }

  const permissions = snapshot.permissions.map((item) => ({ ...item }))
  const index = permission.request_id
    ? permissions.findIndex((item) => item.request_id === permission.request_id)
    : -1

  const entry: RunPermissionSummary = {
    ...permission,
    kind: resolved ? (permission.kind ?? 'resolved') : permission.kind,
    options: permission.options.map((option) => ({ ...option })),
  }

  if (index >= 0) {
    permissions[index] = { ...permissions[index], ...entry }
  } else {
    permissions.push(entry)
  }

  const bounded = permissions.length <= snapshot.limits.permissions
    ? permissions
    : permissions.slice(permissions.length - snapshot.limits.permissions)

  return {
    pending_permission: resolved ? undefined : entry,
    permissions: bounded,
  }
}

function shouldSkipSemanticUpdate(snapshot: RunSnapshot, event: NormalizedRunEvent): boolean {
  if (!event.alias || !event.semantic_fingerprint) return false
  return (
    snapshot._last_semantic_fingerprint === event.semantic_fingerprint &&
    snapshot._last_source_event !== event.source_event
  )
}

function updateDedupeState(snapshot: RunSnapshot, event: NormalizedRunEvent): Pick<RunSnapshot, '_seen_seq_by_scope' | '_recent_legacy_keys'> | null {
  const scope = event.runtime_id ?? event.session_id ?? 'global'
  if (event.seq !== undefined) {
    const seen = snapshot._seen_seq_by_scope[scope] ?? 0
    if (event.seq <= seen) {
      return null
    }
    return {
      _seen_seq_by_scope: { ...snapshot._seen_seq_by_scope, [scope]: event.seq },
      _recent_legacy_keys: snapshot._recent_legacy_keys,
    }
  }

  const key = toLegacyKey(event)
  if (snapshot._recent_legacy_keys.includes(key)) {
    return null
  }

  const nextKeys = [...snapshot._recent_legacy_keys, key]
  return {
    _seen_seq_by_scope: snapshot._seen_seq_by_scope,
    _recent_legacy_keys: nextKeys.length <= snapshot.limits.legacy_keys
      ? nextKeys
      : nextKeys.slice(nextKeys.length - snapshot.limits.legacy_keys),
  }
}

export function reduceRunSnapshot(
  current: RunSnapshot,
  input: unknown,
  limits?: Partial<RunReducerLimits>,
): RunSnapshot {
  const snapshot = limits
    ? { ...current, limits: normalizeLimits({ ...current.limits, ...limits }) }
    : current

  const event = normalizeRunEvent(input)
  if (!event) return snapshot

  const dedupe = updateDedupeState(snapshot, event)
  if (!dedupe) return snapshot

  let next: RunSnapshot = {
    ...snapshot,
    identity: mergeIdentity(snapshot, event),
    latest_seq: event.seq ?? snapshot.latest_seq,
    last_event: event.event,
    last_ts: event.ts ?? snapshot.last_ts,
    phase: event.event === 'session.end' ? 'done' : (event.phase ?? snapshot.phase),
    phase_label: event.phase_label ?? snapshot.phase_label,
    stop_reason: event.stop_reason ?? snapshot.stop_reason,
    usage: event.usage ? { ...(snapshot.usage ?? {}), ...event.usage } : snapshot.usage,
    final_output: clipText(event.final_output ?? snapshot.final_output, snapshot.limits.text_chars),
    _seen_seq_by_scope: dedupe._seen_seq_by_scope,
    _recent_legacy_keys: dedupe._recent_legacy_keys,
  }

  if (event.event === 'session.end') {
    next.ended = true
  }

  if (shouldSkipSemanticUpdate(next, event)) {
    return {
      ...next,
      _last_semantic_fingerprint: event.semantic_fingerprint ?? next._last_semantic_fingerprint,
      _last_source_event: event.source_event,
    }
  }

  switch (event.event) {
    case 'agent.message_chunk': {
      const text = clipText(event.delta, next.limits.text_chars)
      next.assistant_text = appendText(next.assistant_text, text, next.limits.text_chars)
      if (text) {
        next.transcript = appendTranscript(next, {
          kind: 'assistant',
          event: event.event,
          source_event: event.source_event,
          text,
          seq: event.seq,
          ts: event.ts,
        })
      }
      break
    }
    case 'agent.thought_chunk': {
      const text = clipText(event.delta, next.limits.text_chars)
      next.thought_text = appendText(next.thought_text, text, next.limits.text_chars)
      if (text) {
        next.transcript = appendTranscript(next, {
          kind: 'thought',
          event: event.event,
          source_event: event.source_event,
          text,
          seq: event.seq,
          ts: event.ts,
        })
      }
      break
    }
    case 'user.message_chunk': {
      const text = clipText(event.delta, next.limits.text_chars)
      next.user_text = appendText(next.user_text, text, next.limits.text_chars)
      if (text) {
        next.transcript = appendTranscript(next, {
          kind: 'user',
          event: event.event,
          source_event: event.source_event,
          text,
          seq: event.seq,
          ts: event.ts,
        })
      }
      break
    }
    case 'tool.call': {
      next.tools = mergeToolState(next, event, true)
      next.live_tools = liveTools(next.tools)
      next.transcript = appendTranscript(next, {
        kind: 'tool',
        event: event.event,
        source_event: event.source_event,
        title: event.tool?.title ?? event.tool?.kind,
        status: event.tool?.status,
        text: clipText(event.tool?.preview, next.limits.tool_preview_chars),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'tool.call_update': {
      const live = !['completed', 'complete', 'done', 'failed', 'cancelled', 'canceled'].includes((event.tool?.status ?? '').toLowerCase())
      next.tools = mergeToolState(next, event, live)
      next.live_tools = liveTools(next.tools)
      next.transcript = appendTranscript(next, {
        kind: 'tool',
        event: event.event,
        source_event: event.source_event,
        title: event.tool?.title ?? event.tool?.kind,
        status: event.tool?.status,
        text: clipText(event.tool?.preview, next.limits.tool_preview_chars),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'permission.request': {
      const permission = mergePermissionHistory(next, event.permission)
      next.pending_permission = permission.pending_permission
      next.permissions = permission.permissions
      next.phase = 'waiting'
      next.phase_label = event.permission?.question ?? event.permission?.description ?? event.permission?.tool ?? next.phase_label
      next.transcript = appendTranscript(next, {
        kind: 'permission',
        event: event.event,
        source_event: event.source_event,
        request_id: event.permission?.request_id,
        text: clipText(
          event.permission?.question ?? event.permission?.description ?? event.permission?.tool,
          next.limits.text_chars,
        ),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'permission.response': {
      const permission = mergePermissionHistory(next, event.permission ?? next.pending_permission, true)
      next.pending_permission = permission.pending_permission
      next.permissions = permission.permissions
      next.transcript = appendTranscript(next, {
        kind: 'permission',
        event: event.event,
        source_event: event.source_event,
        request_id: event.permission?.request_id ?? next.pending_permission?.request_id,
        text: clipText(event.permission?.kind ?? event.permission?.description, next.limits.text_chars),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'agent.status': {
      next.phase = event.phase ?? next.phase
      next.phase_label = event.phase_label ?? next.phase_label
      next.transcript = appendTranscript(next, {
        kind: 'status',
        event: event.event,
        source_event: event.source_event,
        text: clipText(event.phase_label ?? event.phase, next.limits.text_chars),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'session.end': {
      next.live_tools = []
      next.tools = next.tools.map((tool) => ({ ...tool, live: false }))
      if (!next.final_output) {
        next.final_output = clipText(next.assistant_text, next.limits.text_chars)
      }
      next.pending_permission = undefined
      next.transcript = appendTranscript(next, {
        kind: 'session',
        event: event.event,
        source_event: event.source_event,
        text: clipText(event.final_output ?? event.stop_reason, next.limits.text_chars),
        status: event.stop_reason,
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
    case 'session.start': {
      next.ended = false
      next.transcript = appendTranscript(next, {
        kind: 'session',
        event: event.event,
        source_event: event.source_event,
        text: clipText(event.backend, next.limits.text_chars),
        seq: event.seq,
        ts: event.ts,
      })
      break
    }
  }

  return {
    ...next,
    _last_semantic_fingerprint: event.semantic_fingerprint ?? next._last_semantic_fingerprint,
    _last_source_event: event.source_event,
  }
}

export type RunSnapshotSeed = Partial<
  Omit<
    RunSnapshot,
    '_seen_seq_by_scope' | '_recent_legacy_keys' | '_last_semantic_fingerprint' | '_last_source_event'
  >
>

export class RunReducer {
  private state: RunSnapshot

  constructor(limits?: Partial<RunReducerLimits>, initial?: RunSnapshotSeed) {
    const base = createRunSnapshot(limits)
    if (!initial) {
      this.state = base
      return
    }

    const identity = { ...base.identity, ...(initial.identity ?? {}) }
    const scope = identity.runtime_id ?? identity.session_id ?? 'global'
    const seenSeq = initial.latest_seq === undefined
      ? {}
      : { [scope]: initial.latest_seq }
    this.state = {
      ...base,
      ...initial,
      identity,
      limits: base.limits,
      ended: initial.ended ?? base.ended,
      assistant_text: initial.assistant_text ?? base.assistant_text,
      thought_text: initial.thought_text ?? base.thought_text,
      user_text: initial.user_text ?? base.user_text,
      transcript: (initial.transcript ?? base.transcript).map((entry) => ({ ...entry })),
      tools: (initial.tools ?? base.tools).map((tool) => ({ ...tool })),
      live_tools: (initial.live_tools ?? base.live_tools).map((tool) => ({ ...tool })),
      pending_permission: initial.pending_permission
        ? {
            ...initial.pending_permission,
            options: initial.pending_permission.options.map((option) => ({ ...option })),
          }
        : undefined,
      permissions: (initial.permissions ?? base.permissions).map((permission) => ({
        ...permission,
        options: permission.options.map((option) => ({ ...option })),
      })),
      _seen_seq_by_scope: seenSeq,
      _recent_legacy_keys: [],
    }
  }

  apply(input: unknown): RunSnapshot {
    this.state = reduceRunSnapshot(this.state, input)
    return this.snapshot()
  }

  snapshot(): RunSnapshot {
    return {
      ...this.state,
      identity: { ...this.state.identity },
      usage: this.state.usage ? { ...this.state.usage } : undefined,
      transcript: this.state.transcript.map((entry) => ({ ...entry })),
      tools: this.state.tools.map((tool) => ({ ...tool })),
      live_tools: this.state.live_tools.map((tool) => ({ ...tool })),
      pending_permission: this.state.pending_permission
        ? {
            ...this.state.pending_permission,
            options: this.state.pending_permission.options.map((option) => ({ ...option })),
          }
        : undefined,
      permissions: this.state.permissions.map((permission) => ({
        ...permission,
        options: permission.options.map((option) => ({ ...option })),
      })),
      _seen_seq_by_scope: { ...this.state._seen_seq_by_scope },
      _recent_legacy_keys: [...this.state._recent_legacy_keys],
    }
  }
}

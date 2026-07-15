import { asRecord, numberField, stringArrayField, stringField } from './value-fields.js'

export type RunChunkRole = 'assistant' | 'thought' | 'user'

export interface RunPermissionOption {
  option_id: string
  label: string
  kind: string
}

export interface RunPermissionSummary {
  request_id?: string
  description?: string
  question?: string
  tool?: string
  options: RunPermissionOption[]
  kind?: string
}

export interface RunToolSummary {
  key: string
  id?: string
  title?: string
  kind?: string
  status?: string
  preview?: string
}

export interface NormalizedRunEvent {
  event: string
  source_event: string
  raw: Record<string, unknown>
  runtime_id?: string
  session_id?: string
  run_id?: string
  run_label?: string
  seq?: number
  ts?: number
  phase?: string
  phase_label?: string
  stop_reason?: string
  final_output?: string
  usage?: Record<string, unknown>
  backend?: string
  agent?: string
  model?: string
  dir?: string
  parent_id?: string
  children?: string[]
  event_path?: string
  delta?: string
  chunk_role?: RunChunkRole
  tool?: RunToolSummary
  permission?: RunPermissionSummary
  alias?: boolean
  semantic_fingerprint?: string
}

const MAX_TEXT_EXTRACTION_DEPTH = 12
const MAX_TEXT_EXTRACTION_ITEMS = 64

export function extractEventText(value: unknown, depth = 0): string | undefined {
  if (depth >= MAX_TEXT_EXTRACTION_DEPTH) return undefined
  if (typeof value === 'string') {
    return value.length > 0 ? value : undefined
  }

  if (Array.isArray(value)) {
    const parts: string[] = []
    for (const item of value.slice(0, MAX_TEXT_EXTRACTION_ITEMS)) {
      const text = extractEventText(item, depth + 1)
      if (text) parts.push(text)
    }
    return parts.length > 0 ? parts.join('') : undefined
  }

  const record = asRecord(value)
  if (!record) return undefined

  for (const key of ['delta', 'text', 'content', 'partial', 'message', 'assistantMessage', 'assistantMessageEvent', 'data', 'result', 'output']) {
    const text = extractEventText(record[key], depth + 1)
    if (text) return text
  }

  return undefined
}

function normalizePermissionOptions(value: unknown): RunPermissionOption[] {
  if (!Array.isArray(value)) return []
  const options: RunPermissionOption[] = []
  for (const item of value) {
    const record = asRecord(item)
    if (!record) continue
    const option_id = stringField(record, 'option_id', 'optionId') ?? ''
    const label = stringField(record, 'label') ?? option_id
    const kind = stringField(record, 'kind') ?? ''
    if (!option_id && !label && !kind) continue
    options.push({ option_id, label, kind })
  }
  return options
}

function normalizePermission(record: Record<string, unknown>): RunPermissionSummary | undefined {
  const request_id = stringField(record, 'request_id', 'requestID', 'id')
  const description = stringField(record, 'description', 'title')
  const question = stringField(record, 'question', 'message')
  const tool = stringField(record, 'tool')
  const options = normalizePermissionOptions(record.options)
  const kind = stringField(record, 'kind', 'status')

  if (!request_id && !description && !question && !tool && options.length === 0 && !kind) {
    return undefined
  }

  return { request_id, description, question, tool, options, kind }
}

function normalizeTool(record: Record<string, unknown>, fallbackStatus?: string): RunToolSummary | undefined {
  const title = stringField(record, 'title', 'toolName', 'name')
  const kind = stringField(record, 'kind', 'toolKind')
  const id = stringField(
    record,
    'tool_call_id',
    'toolCallId',
    'call_id',
    'callId',
    'id',
    'message_id',
    'messageID',
    'part_id',
    'partID',
  )
  const status = stringField(record, 'status', 'state') ?? fallbackStatus
  const preview =
    extractEventText(record.preview) ??
    extractEventText(record.arguments) ??
    extractEventText(record.args) ??
    extractEventText(record.input) ??
    extractEventText(record.result) ??
    extractEventText(record.output) ??
    extractEventText(record.delta)

  if (!id && !title && !kind && !status && !preview) {
    return undefined
  }

  const key = id ?? title ?? kind ?? 'tool'
  return { key, id, title, kind, status, preview }
}

function semanticFingerprint(event: string, delta?: string, tool?: RunToolSummary, permission?: RunPermissionSummary, stopReason?: string): string | undefined {
  if (delta) {
    return `${event}:${delta.length}:${delta.slice(0, 512)}`
  }
  if (tool) {
    return `${event}:${tool.id ?? tool.title ?? tool.kind ?? tool.key}:${tool.status ?? ''}:${tool.preview ?? ''}`
  }
  if (permission) {
    return `${event}:${permission.request_id ?? permission.question ?? permission.description ?? permission.tool ?? ''}:${permission.kind ?? ''}`
  }
  if (stopReason) {
    return `${event}:${stopReason}`
  }
  return undefined
}

export function normalizeRunEvent(input: unknown): NormalizedRunEvent | null {
  const raw = asRecord(input)
  if (!raw) return null

  const source_event = stringField(raw, 'event', 'type')
  if (!source_event) return null

  const session_id = stringField(raw, 'session_id', 'sessionID', 'sessionId')
  const runtime_id = stringField(raw, 'runtime_id', 'runtimeID')
  const run_id = stringField(raw, 'run_id', 'runId')
  const run_label = stringField(raw, 'run_label', 'runLabel', 'label')
  const seq = numberField(raw, 'seq')
  const ts = numberField(raw, 'ts', 'timestamp')
  const phase = stringField(raw, 'phase', 'status')
  const phase_label = stringField(raw, 'phase_label', 'label')
  const stop_reason = stringField(raw, 'stop_reason', 'stopReason')
  const final_output = stringField(raw, 'final_output', 'finalOutput')
  const backend = stringField(raw, 'backend')
  const agent = stringField(raw, 'agent')
  const model = stringField(raw, 'model')
  const dir = stringField(raw, 'dir')
  const parent_id = stringField(raw, 'parent_id', 'parentId')
  const children = stringArrayField(raw, 'children')
  const event_path = stringField(raw, 'event_path', 'on_event')
  const usage = asRecord(raw.usage) ?? undefined

  let event = source_event
  let alias = false
  let delta: string | undefined
  let chunk_role: RunChunkRole | undefined
  let tool: RunToolSummary | undefined
  let permission: RunPermissionSummary | undefined

  switch (source_event) {
    case 'agent.message_chunk':
      chunk_role = 'assistant'
      delta = extractEventText(raw)
      break
    case 'agent.thought_chunk':
      chunk_role = 'thought'
      delta = extractEventText(raw)
      break
    case 'user.message_chunk':
      chunk_role = 'user'
      delta = extractEventText(raw)
      break
    case 'tool.call':
      tool = normalizeTool(raw, 'running')
      break
    case 'tool.call_update':
      tool = normalizeTool(raw)
      break
    case 'permission.request':
    case 'permission.response':
      permission = normalizePermission(raw)
      break
    case 'avenor.message.delta':
      event = 'agent.message_chunk'
      alias = true
      chunk_role = 'assistant'
      delta = extractEventText(raw)
      break
    case 'avenor.message.update': {
      alias = true
      const assistantMessageEvent = asRecord(raw.assistantMessageEvent) ?? raw
      const assistantType = stringField(assistantMessageEvent, 'type')
      if (assistantType === 'thinking_delta') {
        event = 'agent.thought_chunk'
        chunk_role = 'thought'
        delta = extractEventText(assistantMessageEvent)
      } else if (assistantType === 'toolcall_start') {
        event = 'tool.call'
        tool = normalizeTool(assistantMessageEvent, 'running')
      } else if (
        assistantType === 'toolcall_delta' ||
        assistantType === 'toolcall_update' ||
        assistantType === 'toolcall_end'
      ) {
        event = 'tool.call_update'
        tool = normalizeTool(
          assistantMessageEvent,
          assistantType === 'toolcall_end' ? 'completed' : 'running',
        )
      } else {
        event = 'agent.message_chunk'
        chunk_role = 'assistant'
        delta = extractEventText(assistantMessageEvent)
      }
      break
    }
    case 'avenor.tool.start':
      event = 'tool.call'
      alias = true
      tool = normalizeTool(raw, 'running')
      break
    case 'avenor.tool.update':
      event = 'tool.call_update'
      alias = true
      tool = normalizeTool(raw)
      break
    case 'avenor.tool.end':
      event = 'tool.call_update'
      alias = true
      tool = normalizeTool(raw, 'completed')
      break
  }

  return {
    event,
    source_event,
    raw: { ...raw },
    runtime_id,
    session_id,
    run_id,
    run_label,
    seq,
    ts,
    phase,
    phase_label,
    stop_reason,
    final_output,
    usage,
    backend,
    agent,
    model,
    dir,
    parent_id,
    children,
    event_path,
    delta,
    chunk_role,
    tool,
    permission,
    alias,
    semantic_fingerprint: semanticFingerprint(event, delta, tool, permission, stop_reason),
  }
}

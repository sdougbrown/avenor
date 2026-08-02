import { extractEventText } from '@dougbots/avenor-core'
import type { InspectResult, ResultResult, StatusResult } from '@dougbots/avenor-core'

const FINAL_OUTPUT_CHARS = 1_500
const SCALAR_CHARS = 240
const EVENT_CHARS = 600
const PREVIEW_LINES = 12
const STATUS_ROWS = 12
const COLLECTION_ITEMS = 8
const INSPECT_ITEMS = 5
const CLEANUP_ITEMS = 8
const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])

type RichToolResult = {
  title: string
  output: string
  metadata: Record<string, unknown>
}

type RecordValue = Record<string, unknown>

type StatusArgs = { run_id?: string; view?: string; supervisor_id?: string }
type ResultArgs = { run_id?: string; wait?: boolean; timeout?: string; supervisor_id?: string }
type InspectArgs = { run_id?: string; limit?: number; after_seq?: number; supervisor_id?: string }
type PermissionArgs = { run_id?: string; option_id?: string; request_id?: string; message?: string; supervisor_id?: string }
type FollowUpArgs = { run_id?: string; message?: string; label?: string; supervisor_id?: string }
type EventsArgs = { run_id?: string; types?: string[]; limit?: number; supervisor_id?: string }
type ShutdownArgs = { supervisor_id?: string; force?: boolean }

const ANSI_PATTERN =
  // eslint-disable-next-line no-control-regex
  /[\u001B\u009B][[\]()#;?]*(?:(?:(?:[a-zA-Z\d]*(?:;[a-zA-Z\d]*)*)?\u0007)|(?:(?:\d{1,4}(?:;\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~]))/g

function asRecord(value: unknown): RecordValue | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as RecordValue
    : undefined
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value : undefined
}

function numberValue(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim() && Number.isFinite(Number(value))) return Number(value)
  return undefined
}

function sanitizeText(text: string): string {
  return text
    .replace(ANSI_PATTERN, '')
    .replaceAll('\t', '  ')
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, '')
}

function itemMarker(count: number): string {
  return `… ${count} items omitted`
}

function lineMarker(count: number): string {
  return `… ${count} lines omitted`
}

function characterMarker(count: number): string {
  return `[preview clipped: ${count} characters omitted]`
}

function cleanMultiline(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const clean = sanitizeText(value).trim()
  return clean || undefined
}

function tailClip(value: string, limit: number): { text: string; omitted?: number } {
  if (value.length <= limit) return { text: value }

  let markerLength = characterMarker(value.length).length
  for (let attempt = 0; attempt < 3; attempt++) {
    const visibleLength = Math.max(0, limit - markerLength)
    const nextMarkerLength = characterMarker(value.length - visibleLength).length
    if (nextMarkerLength === markerLength) {
      return { text: value.slice(value.length - visibleLength), omitted: value.length - visibleLength }
    }
    markerLength = nextMarkerLength
  }
  const visibleLength = Math.max(0, limit - markerLength)
  return { text: value.slice(value.length - visibleLength), omitted: value.length - visibleLength }
}

function scalar(value: unknown, fallback = 'unknown', limit = SCALAR_CHARS): string {
  const clean = cleanMultiline(typeof value === 'string' ? value : undefined)
  if (!clean) return fallback
  const oneLine = clean.replace(/[\r\n]+/g, ' ').replace(/\s+/g, ' ').trim()
  const clipped = tailClip(oneLine, limit)
  return clipped.omitted === undefined ? clipped.text : `${clipped.text}${characterMarker(clipped.omitted)}`
}

function optionalScalar(value: unknown, limit = SCALAR_CHARS): string | undefined {
  const clean = cleanMultiline(typeof value === 'string' ? value : undefined)
  return clean ? scalar(clean, 'unknown', limit) : undefined
}

function preview(value: unknown, limit: number): string[] | undefined {
  const clean = cleanMultiline(value)
  if (!clean) return undefined
  const clipped = tailClip(clean, limit)
  const lines = clipped.text.split('\n').slice(-PREVIEW_LINES)
  const omittedLines = Math.max(0, clean.split('\n').length - lines.length)
  return [
    ...lines,
    ...(clipped.omitted === undefined ? [] : [characterMarker(clipped.omitted)]),
    ...(omittedLines === 0 ? [] : [lineMarker(omittedLines)]),
  ]
}

function selected(record: RecordValue | undefined, key: string, fallback?: unknown, defaultValue = 'unknown'): string {
  return scalar(stringValue(record?.[key]) ?? stringValue(fallback), defaultValue)
}

function selectedOptional(record: RecordValue | undefined, key: string): string | undefined {
  return optionalScalar(record?.[key])
}

function hasStatusIdentity(record: RecordValue | undefined): boolean {
  return !!record && !!(stringValue(record.run_id) || stringValue(record.label) || stringValue(record.status))
}

function fallback(tool: string): string[] {
  return [
    `Avenor ${tool} result unavailable.`,
    'Retry the tool or use avenor_status/avenor_inspect.',
  ]
}

function output(lines: string[] | undefined, tool: string): string {
  return (lines ?? fallback(tool)).join('\n')
}

function isTerminal(status: string): boolean {
  return TERMINAL_STATUSES.has(status)
}

function permission(record: RecordValue | undefined): RecordValue | undefined {
  return asRecord(record?.pending_permission)
}

function permissionOptions(record: RecordValue | undefined): RecordValue[] {
  const options = record?.options
  return Array.isArray(options) ? options.map(asRecord).filter((value): value is RecordValue => !!value) : []
}

function optionRows(options: RecordValue[]): string {
  return options
    .slice(0, COLLECTION_ITEMS)
    .map(option => `${selected(option, 'option_id')} (${selected(option, 'label')})`)
    .join(', ')
}

function optionLine(prefix: string, options: RecordValue[]): string {
  const rows = optionRows(options)
  const omitted = options.length - Math.min(options.length, COLLECTION_ITEMS)
  return `${prefix}${rows ? ` ${rows}` : ''}${omitted > 0 ? ` ${itemMarker(omitted)}` : ''}`
}

function guidance(status: string, pending: RecordValue | undefined, runId: string): string {
  if (status === 'waiting') {
    const options = permissionOptions(pending)
    if (options.length > 0) {
      return `Guidance: Call avenor_answer_permission with run_id "${runId}" and option_id "${selected(options[0], 'option_id')}".`
    }
    if (pending) {
      return `Guidance: Permission input is required, but no options are available; call avenor_inspect with run_id "${runId}".`
    }
    return `Guidance: Call avenor_follow_up or avenor_status with run_id "${runId}".`
  }
  if (isTerminal(status)) return `Guidance: Call avenor_result or avenor_events with run_id "${runId}".`
  return `Guidance: Call avenor_result with run_id "${runId}".`
}

function statusExpanded(status: RecordValue): string[] {
  const fields: Array<[string, string]> = [
    ['Phase', 'phase'],
    ['Phase label', 'phase_label'],
    ['Session', 'session_id'],
    ['Backend', 'backend'],
    ['Agent', 'agent'],
    ['Model', 'model'],
    ['Latest sequence', 'latest_seq'],
    ['Stop reason', 'stop_reason'],
  ]
  return fields.flatMap(([label, key]) => {
    const value = key === 'latest_seq' ? numberValue(status[key])?.toString() : selectedOptional(status, key)
    return value === undefined ? [] : [`${label}: ${value}`]
  })
}

function statusLines(details: unknown, args: StatusArgs): string[] | undefined {
  if (Array.isArray(details)) {
    const statuses = details.map(asRecord)
    if (statuses.some(status => !hasStatusIdentity(status))) return undefined
    const displayed = statuses.slice(0, STATUS_ROWS) as RecordValue[]
    const lines = [
      `Avenor status — ${statuses.length} runs`,
      ...displayed.map(status => {
        const runId = selected(status, 'run_id', args.run_id)
        return `${selected(status, 'label', undefined, runId)} — ${selected(status, 'status')} (run_id: ${runId})`
      }),
      ...(statuses.length > displayed.length ? [itemMarker(statuses.length - displayed.length)] : []),
    ]
    for (const status of displayed) {
      const runId = selected(status, 'run_id', args.run_id)
      lines.push(
        `Run: ${runId}`,
        `Label: ${selected(status, 'label', undefined, runId)}`,
        `Status: ${selected(status, 'status')}`,
        ...statusExpanded(status),
      )
    }
    return lines
  }

  const status = asRecord(details)
  if (!hasStatusIdentity(status)) return undefined
  const runId = selected(status, 'run_id', args.run_id)
  const label = selected(status, 'label', undefined, runId)
  const state = selected(status, 'status')
  const pending = permission(status)
  const options = permissionOptions(pending)
  const finalPreview = preview(status.final_output, FINAL_OUTPUT_CHARS)
  return [
    `Status: ${label} — ${state}`,
    `Run: ${runId}`,
    ...(pending ? [`Permission: ${selected(pending, 'description')}`, optionLine('Options:', options)] : []),
    ...(status.final_output_truncated === true
      ? [`Warning: final output preview is truncated; call avenor_result with run_id "${runId}".`]
      : []),
    ...(finalPreview ? ['Final output:', ...finalPreview] : ['Final output: unavailable.']),
    guidance(state, pending, runId),
    ...statusExpanded(status),
  ]
}

function resultLines(details: unknown, args: ResultArgs): string[] | undefined {
  const result = asRecord(details)
  if (!hasStatusIdentity(result)) return undefined
  const runId = selected(result, 'run_id', args.run_id)
  const label = selected(result, 'label', undefined, runId)
  const state = selected(result, 'status')
  const pending = permission(result)
  const finalPreview = preview(result.output, FINAL_OUTPUT_CHARS)
  const session = selectedOptional(result, 'session_id')
  const stopReason = selectedOptional(result, 'stop_reason')
  return [
    `Result: ${label} — ${state}`,
    `Run: ${runId}`,
    ...(result.timed_out === true ? ['Warning: wait timed out; the run is still active.'] : []),
    ...(pending ? [`Permission: ${selected(pending, 'description')}`] : []),
    guidance(state, pending, runId),
    ...(result.output_truncated === true
      ? [stringValue(result.output_event_path)
        ? `Warning: final output may be truncated; retry avenor_result or read the durable event path "${scalar(result.output_event_path)}".`
        : `Warning: final output may be truncated; call avenor_events with run_id "${runId}".`]
      : []),
    ...(finalPreview ? ['Final output:', ...finalPreview] : ['Final output: unavailable.']),
    ...(session ? [`Session: ${session}`] : []),
    ...(stopReason ? [`Stop reason: ${stopReason}`] : []),
  ]
}

function permissionLines(details: unknown, args: PermissionArgs): string[] | undefined {
  const result = asRecord(details)
  if (!result || typeof result.ok !== 'boolean') return undefined
  const runId = scalar(args.run_id)
  return [
    `Permission answer: ${result.ok ? 'accepted' : 'failed'}.`,
    `Run: ${runId}`,
    `Option: ${scalar(args.option_id)}`,
    ...(stringValue(args.request_id) ? [`Request: ${scalar(args.request_id)}`] : []),
    `Guidance: Call avenor_status with run_id "${runId}" to verify the permission state.`,
  ]
}

function followUpLines(details: unknown, args: FollowUpArgs): string[] | undefined {
  const result = asRecord(details)
  if (!result || !stringValue(result.run_id) || !stringValue(result.label)) return undefined
  const runId = selected(result, 'run_id', args.run_id)
  const label = selected(result, 'label', args.label, runId)
  return [
    `Follow-up: ${label} — dispatched.`,
    `Source run: ${scalar(args.run_id)}`,
    `Run: ${runId}`,
    `Label: ${label}`,
    `Guidance: Call avenor_status with run_id "${runId}".`,
  ]
}

function eventSequence(value: unknown): string | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return String(value)
  return typeof value === 'string' && value.trim() && Number.isFinite(Number(value)) ? value : undefined
}

function eventRow(value: RecordValue): string {
  const name = stringValue(value.event) ?? stringValue(value.type) ?? 'unknown'
  const sequence = eventSequence(value.seq)
  const text = extractEventText(value)
  const previewText = text ? scalar(text, 'unknown', EVENT_CHARS) : undefined
  return `Event ${sequence ?? '?'}: ${scalar(name)}${previewText ? ` — ${previewText}` : ''}`
}

function eventsLines(details: unknown, args: EventsArgs): string[] | undefined {
  const result = asRecord(details)
  if (!result || !Array.isArray(result.events) || result.events.some(event => !asRecord(event))) return undefined
  const events = result.events.map(event => event as RecordValue)
  const runId = scalar(args.run_id)
  const types = Array.isArray(args.types) ? args.types : []
  const displayedTypes = types.slice(0, COLLECTION_ITEMS).map(type => scalar(type)).join(', ')
  const lines = [
    `Events: ${runId} — ${events.length} events`,
    ...(types.length > 0 ? [`Filter: ${displayedTypes}${types.length > COLLECTION_ITEMS ? ` ${itemMarker(types.length - COLLECTION_ITEMS)}` : ''}`] : []),
  ]
  if (events.length === 0) return [...lines, 'No events.']
  const displayed = events.slice(0, STATUS_ROWS)
  lines.push(...displayed.map(eventRow))
  if (events.length > displayed.length) lines.push(itemMarker(events.length - displayed.length))
  return lines
}

function inspectTranscriptRow(value: RecordValue): string | undefined {
  const kind = stringValue(value.kind)
  const text = optionalScalar(value.text)
  if (!kind) return undefined
  if (kind === 'tool') {
    const title = selected(value, 'title', stringValue(value.kind) ?? stringValue(value.key))
    const status = optionalScalar(value.status)
    return `${title}${status ? ` (${status})` : ''}${text ? `: ${text}` : ''}`
  }
  if (!text) return undefined
  if (['assistant', 'thought', 'user', 'permission', 'status', 'session'].includes(kind)) return `${kind}: ${text}`
  return undefined
}

function inspectToolRow(prefix: string, value: RecordValue): string {
  const title = selected(value, 'title', stringValue(value.kind) ?? stringValue(value.key))
  const status = optionalScalar(value.status)
  const text = optionalScalar(value.preview)
  return `${prefix}: ${title}${status ? ` (${status})` : ''}${text ? `: ${text}` : ''}`
}

function inspectPermissionRow(value: RecordValue): string {
  const description = scalar(
    stringValue(value.description)
      ?? stringValue(value.question)
      ?? stringValue(value.tool)
      ?? stringValue(value.kind),
  )
  const options = permissionOptions(value)
  const rows = optionRows(options)
  return `Permission: ${description} (request_id: ${selected(value, 'request_id')}); options:${rows ? ` ${rows}` : ''}${options.length > COLLECTION_ITEMS ? ` ${itemMarker(options.length - COLLECTION_ITEMS)}` : ''}`
}

function inspectGroup(title: string, values: unknown, row: (value: RecordValue) => string | undefined): string[] {
  const records = Array.isArray(values) ? values.map(asRecord).filter((value): value is RecordValue => !!value) : []
  const rows = records.map(row).filter((value): value is string => !!value)
  return [
    `${title}:`,
    ...rows.slice(0, INSPECT_ITEMS),
    ...(rows.length > INSPECT_ITEMS ? [itemMarker(rows.length - INSPECT_ITEMS)] : []),
  ]
}

function inspectLines(details: unknown, args: InspectArgs): string[] | undefined {
  const inspect = asRecord(details)
  const status = asRecord(inspect?.status)
  if (!inspect || !status || (!hasStatusIdentity(inspect) && !hasStatusIdentity(status))) return undefined
  const runId = selected(inspect, 'run_id', args.run_id)
  const label = selected(inspect, 'label', undefined, runId)
  const state = selected(status, 'status')
  const finalPreview = preview(inspect.final_output, FINAL_OUTPUT_CHARS)
  const transcript = Array.isArray(inspect.transcript) ? inspect.transcript : []
  const tools = Array.isArray(inspect.tools) ? inspect.tools : []
  const liveTools = Array.isArray(inspect.live_tools) ? inspect.live_tools : []
  const permissions = Array.isArray(inspect.permissions) ? inspect.permissions : []
  return [
    `Inspect: ${label} — ${state}`,
    `Run: ${runId}`,
    `Counts: transcript=${transcript.length}, tools=${tools.length}, live_tools=${liveTools.length}, permissions=${permissions.length}`,
    ...(finalPreview ? ['Final output:', ...finalPreview] : ['Final output: unavailable.']),
    `Guidance: Call avenor_events with run_id "${runId}" for raw events.`,
    ...inspectGroup('Transcript', transcript, inspectTranscriptRow),
    ...inspectGroup('Tools', tools, value => inspectToolRow('Tool', value)),
    ...inspectGroup('Live tools', liveTools, value => inspectToolRow('Live tool', value)),
    ...inspectGroup('Permissions', permissions, inspectPermissionRow),
  ]
}

function shutdownLines(details: unknown, args: ShutdownArgs): string[] | undefined {
  const result = asRecord(details)
  if (!result || typeof result.ok !== 'boolean' || !Array.isArray(result.cleaned_up)) return undefined
  const paths = result.cleaned_up.filter((path): path is string => typeof path === 'string')
  const supervisor = stringValue(args.supervisor_id) ? scalar(args.supervisor_id) : 'singleton'
  return [
    `Avenor shutdown: ${result.ok ? 'succeeded' : 'failed'}.`,
    `Supervisor: ${supervisor}`,
    `Mode: ${args.force === true ? 'force' : 'graceful'}`,
    paths.length > 0 ? `Cleanup: ${paths.length} paths` : 'Cleanup: none',
    ...(!result.ok ? ['Warning: shutdown did not complete.', 'Guidance: retry avenor_shutdown.'] : []),
    ...paths.slice(0, CLEANUP_ITEMS).map(path => `Cleanup path: ${scalar(path)}`),
    ...(paths.length > CLEANUP_ITEMS ? [itemMarker(paths.length - CLEANUP_ITEMS)] : []),
  ]
}

export function formatStatusOutput(args: StatusArgs, result: StatusResult | StatusResult[]): RichToolResult {
  const statuses = Array.isArray(result) ? result : undefined
  if (statuses) {
    return {
      title: `Avenor status — ${statuses.length} runs`,
      output: output(statusLines(statuses, args), 'status'),
      metadata: { results: statuses, count: statuses.length },
    }
  }
  const record = result as RecordValue
  const runId = selected(record, 'run_id', args.run_id)
  const label = selected(record, 'label', undefined, runId)
  const state = selected(record, 'status')
  return {
    title: `${label} — ${state}`,
    output: output(statusLines(result, args), 'status'),
    metadata: { ...result },
  }
}

export function formatResultOutput(args: ResultArgs, result: ResultResult): RichToolResult {
  const record = result as RecordValue
  const runId = selected(record, 'run_id', args.run_id)
  const label = selected(record, 'label', undefined, runId)
  const state = selected(record, 'status')
  return {
    title: `${label} — ${state}${result.timed_out === true ? ' (wait timed out)' : ''}`,
    output: output(resultLines(result, args), 'result'),
    metadata: { ...result },
  }
}

export function formatAnswerPermissionOutput(args: PermissionArgs, result: { ok: boolean }): RichToolResult {
  const runId = scalar(args.run_id)
  return {
    title: `${runId} — permission answered`,
    output: output(permissionLines(result, args), 'answer_permission'),
    metadata: {
      ...result,
      run_id: args.run_id,
      option_id: args.option_id,
      request_id: args.request_id,
    },
  }
}

export function formatFollowUpOutput(args: FollowUpArgs, result: { run_id: string; label: string }): RichToolResult {
  const record = result as RecordValue
  const runId = selected(record, 'run_id', args.run_id)
  const label = selected(record, 'label', args.label, runId)
  return {
    title: `${label} — dispatched`,
    output: output(followUpLines(result, args), 'follow_up'),
    metadata: { ...result, source_run_id: args.run_id },
  }
}

export function formatEventsOutput(args: EventsArgs, result: { events: Array<Record<string, unknown>> }): RichToolResult {
  const runId = scalar(args.run_id)
  return {
    title: `${runId} — ${result.events.length} events`,
    output: output(eventsLines(result, args), 'events'),
    metadata: { ...result, run_id: args.run_id, count: result.events.length },
  }
}

export function formatInspectOutput(args: InspectArgs, result: InspectResult): RichToolResult {
  const record = result as unknown as RecordValue
  const status = result.status as unknown as RecordValue
  const runId = selected(record, 'run_id', args.run_id)
  const label = selected(record, 'label', undefined, runId)
  return {
    title: `${label} — ${selected(status, 'status')}`,
    output: output(inspectLines(result, args), 'inspect'),
    metadata: { ...result },
  }
}

export function formatShutdownOutput(args: ShutdownArgs, result: { ok: boolean; cleaned_up: string[] }): RichToolResult {
  return {
    title: 'Avenor — shut down',
    output: output(shutdownLines(result, args), 'shutdown'),
    metadata: {
      ...result,
      supervisor_id: args.supervisor_id,
      force: args.force ?? false,
    },
  }
}

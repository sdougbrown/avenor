// The TypeScript router for the merged messaging tools. It runs the same four
// phases as internal/messaging/router, in the same order: admission, resolution,
// permission, then state and the provider call. The admission tables, repair
// sentences, outcome text and route preconditions all come from schemas/, and
// the conformance fixture is the same one the Go router runs, so both languages
// reject and render identical inputs identically.
import { umpire } from '@umpire/core'
import { fromJson, type UmpireJsonSchema } from '@umpire/json'

import messageInputSchema from '../../../schemas/message.input.json'
import messageRouteSchema from '../../../schemas/message_route.umpire.json'
import conformanceFixture from '../../../schemas/message_route.conformance.json'
import peersInputSchema from '../../../schemas/peers.input.json'
import {
  isVocabularyOutcome,
  quote,
  quoteList,
  renderLast,
  renderOutcome,
  repair,
  rendersOutcome,
  sanitize,
  type MessageOutcome,
} from './messaging-text.js'
import type { MessageProvider, PeerRecord, ProviderResult } from './tools/message.js'

/** A typed tool denial: one fixed code and the rendered sentence. */
export class ToolError extends Error {
  readonly code: string
  readonly facts: Record<string, string>

  constructor(code: string, message: string, facts: Record<string, string> = {}) {
    super(message)
    this.name = 'ToolError'
    this.code = code
    this.facts = facts
  }
}

export interface MessageInput {
  action: 'tell' | 'ask' | 'reply' | 'resume' | 'withdraw' | 'control_reply'
  target?: string
  message?: string
  reply_to?: string
  message_id?: string
  timeout_ms?: number
  label?: string
  idempotency_key?: string
  payload?: Record<string, unknown>
}

export interface PeersInput {
  action?: 'list' | 'rename' | 'last'
  name?: string
  session_id?: string
  target?: string
}

export interface PeersRouterResult {
  action: 'list' | 'rename' | 'last'
  outcome?: MessageOutcome
  text: string
  peers: PeerRecord[]
  renamed?: { run_id: string; label: string; previous_label: string }
  last?: {
    run_id: string
    runtime_id: string
    label: string
    final_output: string
    stop_reason: string
    final_output_truncated: boolean
  }
}

interface ApplicabilityEntry {
  permitted: string[]
  required?: string[]
  types?: Record<string, string>
}

interface StringConstraints {
  minLength?: number
  maxLength?: number
  pattern?: RegExp
}

interface IntegerConstraints {
  minimum?: number
  maximum?: number
}

interface RouteConditions {
  callerKind: string
  surface: string
  targetLive: boolean
  targetResumable: boolean
  targetMatch: string
  controlReplySupported: boolean
}

interface FieldStatus {
  enabled: boolean
  fair: boolean
  reason: string | null
  reasons: string[]
}

const fixture = conformanceFixture as unknown as {
  applicability: Record<string, ApplicabilityEntry>
  peers_applicability: Record<string, ApplicabilityEntry>
}

const messageApplicability = fixture.applicability
const peersApplicability = fixture.peers_applicability

type RouteRule = {
  type: string
  reason?: string
  when?: RouteExpr
  rules?: RouteRule[]
  branches?: Record<string, RouteRule[]>
}

type RouteExpr = {
  op: string
  value?: unknown
  values?: unknown
  expr?: RouteExpr
  exprs?: RouteExpr[]
}

/**
 * Normalize the Umpire JSON dialect the shared schema uses onto the one
 * @umpire/json compiles: `in` carries `values` (not `value`) and `not` carries
 * one `expr` (not an `exprs` list). The action-enum `check` rule is dropped;
 * the router enforces that enum from the applicability table and reads the
 * rule's reason directly, and @umpire/json's validator cannot represent an
 * `in` check.
 */
function normalizeRouteExpr(expression: RouteExpr): RouteExpr {
  const normalized: RouteExpr = { ...expression }
  if (normalized.op === 'in' && normalized.values === undefined && normalized.value !== undefined) {
    normalized.values = normalized.value
    delete normalized.value
  }
  if (normalized.op === 'not' && normalized.expr === undefined && normalized.exprs) {
    normalized.expr = normalized.exprs.length === 1
      ? normalized.exprs[0]
      : { op: 'and', exprs: normalized.exprs }
    delete normalized.exprs
  }
  if (normalized.exprs) normalized.exprs = normalized.exprs.map(normalizeRouteExpr)
  if (normalized.expr) normalized.expr = normalizeRouteExpr(normalized.expr)
  return normalized
}

function normalizeRouteRule(rule: RouteRule): RouteRule | undefined {
  if (rule.type === 'check') return undefined
  const normalized: RouteRule = { ...rule }
  if (normalized.when) normalized.when = normalizeRouteExpr(normalized.when)
  if (normalized.rules) {
    normalized.rules = normalized.rules
      .map(normalizeRouteRule)
      .filter((inner): inner is RouteRule => inner !== undefined)
  }
  if (normalized.branches) {
    normalized.branches = Object.fromEntries(
      Object.entries(normalized.branches).map(([branch, rules]) => [branch, rules.map(normalizeRouteRule).filter((inner): inner is RouteRule => inner !== undefined)]),
    )
  }
  return normalized
}

const routeSchema = messageRouteSchema as unknown as { rules: RouteRule[] }
const parsedRoute = fromJson({
  ...(messageRouteSchema as unknown as UmpireJsonSchema),
  rules: routeSchema.rules
    .map(normalizeRouteRule)
    .filter((rule): rule is RouteRule => rule !== undefined),
} as UmpireJsonSchema)
const messageRoute = umpire({ fields: parsedRoute.fields, rules: parsedRoute.rules })
const invalidActionReason = routeSchema.rules.find(rule => rule.type === 'check')?.reason ?? ''

const emptyConditions: RouteConditions = {
  callerKind: '',
  surface: '',
  targetLive: false,
  targetResumable: false,
  targetMatch: '',
  controlReplySupported: false,
}

function routeCheck(
  fields: Record<string, unknown>,
  conditions: RouteConditions,
): Record<string, FieldStatus> {
  // @umpire/core evaluates a field's rules only when the field is present, while
  // the generated Go Check computes them unconditionally. Give every routed
  // field a placeholder value so the same predicates run on both sides; the
  // rules read only `action` and the conditions, never these values.
  const values: Record<string, unknown> = {
    target: 'x',
    message: 'x',
    reply_to: 'x',
    message_id: 'x',
    ...fields,
  }
  return messageRoute.check(values, conditions as unknown as Record<string, unknown>, {}) as unknown as Record<string, FieldStatus>
}

function firstReason(status: FieldStatus | undefined): string {
  if (!status) return ''
  return status.reason ?? status.reasons[0] ?? ''
}

/**
 * Compile the per-property limits the published JSON schemas declare, so the
 * router and the schemas cannot disagree about length, characters or bounds.
 */
function schemaConstraints(schema: unknown): {
  strings: Record<string, StringConstraints>
  integers: Record<string, IntegerConstraints>
} {
  const strings: Record<string, StringConstraints> = {}
  const integers: Record<string, IntegerConstraints> = {}
  const variants = (schema as { oneOf?: Array<{ properties?: Record<string, Record<string, unknown>> }> }).oneOf ?? []
  for (const variant of variants) {
    for (const [name, property] of Object.entries(variant.properties ?? {})) {
      if (typeof property.pattern === 'string') {
        strings[name] = {
          minLength: typeof property.minLength === 'number' ? property.minLength : undefined,
          maxLength: typeof property.maxLength === 'number' ? property.maxLength : undefined,
          pattern: new RegExp(property.pattern, 'u'),
        }
      }
      if (property.type === 'integer') {
        integers[name] = {
          minimum: typeof property.minimum === 'number' ? property.minimum : undefined,
          maximum: typeof property.maximum === 'number' ? property.maximum : undefined,
        }
      }
    }
  }
  return { strings, integers }
}

const messageConstraints = schemaConstraints(messageInputSchema)
const peersConstraints = schemaConstraints(peersInputSchema)

function declaredType(entry: ApplicabilityEntry, name: string): string {
  return entry.types?.[name] ?? 'string'
}

function hasWireType(value: unknown, kind: string): boolean {
  switch (kind) {
    case 'string':
      return typeof value === 'string'
    case 'integer':
      return typeof value === 'number' && Number.isInteger(value)
    case 'object':
      return value !== null && typeof value === 'object' && !Array.isArray(value)
    default:
      return false
  }
}

function wrongTypeRepair(kind: string, name: string): string {
  switch (kind) {
    case 'integer':
      return repair('wrong_type_integer', { name: quote(name) })
    case 'object':
      return repair('wrong_type_object', { name: quote(name) })
    default:
      return repair('wrong_type_string', { name: quote(name) })
  }
}

function validateFields(
  fields: Record<string, unknown>,
  entry: ApplicabilityEntry,
  constraints: { strings: Record<string, StringConstraints>; integers: Record<string, IntegerConstraints> },
): string | null {
  const permitted = new Set(entry.permitted)
  for (const name of Object.keys(fields).sort()) {
    if (!permitted.has(name)) return repair('unknown_property', { name: quote(name) })
  }
  for (const name of entry.required ?? []) {
    if (!(name in fields)) return repair('missing_required_property', { name: quote(name) })
  }
  for (const name of entry.permitted) {
    if (!(name in fields)) continue
    const value = fields[name]
    const kind = declaredType(entry, name)
    if (!hasWireType(value, kind)) return wrongTypeRepair(kind, name)
    if (kind === 'string') {
      const constraint = constraints.strings[name]
      if (!constraint) continue
      const length = Array.from(value as string).length
      if (constraint.minLength !== undefined && length < constraint.minLength) {
        return repair('string_too_short', { name: quote(name), min: String(constraint.minLength) })
      }
      if (constraint.maxLength !== undefined && length > constraint.maxLength) {
        return repair('string_too_long', { name: quote(name), max: String(constraint.maxLength) })
      }
      if (constraint.pattern && !constraint.pattern.test(value as string)) {
        return repair('string_pattern', { name: quote(name) })
      }
    }
    if (kind === 'integer') {
      const constraint = constraints.integers[name]
      if (!constraint) continue
      if (constraint.minimum !== undefined && (value as number) < constraint.minimum) {
        return repair('integer_too_small', { name: quote(name), min: String(constraint.minimum) })
      }
      if (constraint.maximum !== undefined && (value as number) > constraint.maximum) {
        return repair('integer_too_large', { name: quote(name), max: String(constraint.maximum) })
      }
    }
  }
  return null
}

function invalidAction(fields: Record<string, unknown> = {}): ToolError {
  return new ToolError('invalid_action', invalidActionReason)
}

function invalidRequest(action: string, detail: string): ToolError {
  const facts = { action: quote(action), detail }
  return new ToolError('invalid_request', repair('invalid_request', facts), facts)
}

function peersInvalidAction(): ToolError {
  return new ToolError('invalid_action', repair('peers_invalid_action'))
}

/**
 * Phase 1 for avenor_message: the action enum, the action's permitted and
 * required field sets, and each field's declared wire type and limits. The
 * table is the one the Go router validates against.
 */
export function assertMessageInput(input: unknown): asserts input is MessageInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw invalidAction()
  }
  const fields = input as Record<string, unknown>
  const action = fields.action
  if (typeof action !== 'string') throw invalidAction()
  const entry = messageApplicability[action]
  if (!entry) throw invalidAction({ action })
  const detail = validateFields(fields, entry, messageConstraints)
  if (detail) throw invalidRequest(action, detail)
}

/**
 * Phase 1 for avenor_peers. An omitted action means `list`; a present but
 * non-string or unknown action is one invalid action, like the Go router.
 */
export function assertPeersInput(input: unknown): asserts input is PeersInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input)) {
    throw peersInvalidAction()
  }
  const fields = input as Record<string, unknown>
  if ('action' in fields && typeof fields.action !== 'string') throw peersInvalidAction()
  const action = (fields.action as string | undefined) ?? 'list'
  const entry = peersApplicability[action]
  if (!entry) throw peersInvalidAction()
  const detail = validateFields({ ...fields, action }, entry, peersConstraints)
  if (detail) throw invalidRequest(action, detail)
}

function routeConditions(provider: MessageProvider): RouteConditions {
  return {
    callerKind: provider.caller().kind,
    surface: provider.surface(),
    targetLive: false,
    targetResumable: false,
    targetMatch: '',
    controlReplySupported: provider.surface() === 'sidecar',
  }
}

function permissionDenial(action: string, conditions: RouteConditions): ToolError | null {
  if (action === 'control_reply' && !conditions.controlReplySupported) {
    const availability = routeCheck({ action }, conditions)
    return new ToolError('unsupported_action', firstReason(availability.target))
  }
  if (action === 'resume') {
    // The generated fairWhen slot is the allowlist: a resume is fair only when
    // the caller is the owner, so an empty or future caller kind is denied here.
    const availability = routeCheck({ action }, conditions)
    if (!availability.target?.fair) {
      return new ToolError('not_permitted', firstReason(availability.target))
    }
  }
  return null
}

function stateDenial(
  action: string,
  conditions: RouteConditions,
  runId: string,
  status: string,
): ToolError | null {
  if (action !== 'tell' && action !== 'ask' && action !== 'resume') return null
  const availability = routeCheck({ action }, conditions)
  const facts: Record<string, string> = { run_id: quote(runId), status: sanitize(status) }
  if (action === 'tell' || action === 'ask') {
    if (availability.message?.enabled) return null
    facts.repair = conditions.targetResumable
      ? 'use action "resume" to continue it'
      : 'ask the supervisor to continue this run'
    return new ToolError('wrong_action_for_state', repair('wrong_action_for_state', facts), facts)
  }
  if (conditions.targetLive) {
    return new ToolError('target_not_terminal', repair('target_not_terminal', facts), facts)
  }
  if (!availability.message?.fair) {
    return new ToolError('not_resumable', repair('not_resumable', facts), facts)
  }
  return null
}

function refused(err: unknown): ToolError {
  if (err instanceof ToolError) return err
  const error = err as { code?: unknown; message?: unknown }
  const message = typeof error?.message === 'string' ? error.message : String(err)
  // A filesystem error names an absolute path that must not reach a model or a
  // peer, so it is classified to the shared sentence instead of passed through.
  if (error?.code === 'ENOENT' || error?.code === 'EACCES' || error?.code === 'EPERM') {
    return new ToolError('refused', repair('refused_io'))
  }
  return new ToolError('refused', message)
}

function asToolError(err: unknown): ToolError {
  return err instanceof ToolError ? err : refused(err)
}

/**
 * Turn a provider outcome into the contract's text. An outcome outside the
 * vocabulary is reported as possibly-pending; an in-vocabulary outcome the
 * matrix does not list stays unrendered so the gap is visible.
 */
function outcomeText(
  action: string,
  outcome: string,
  facts: Record<string, string>,
): { outcome: MessageOutcome; text: string } {
  if (rendersOutcome(action, outcome)) {
    return { outcome: outcome as MessageOutcome, text: renderOutcome(action, outcome, facts) }
  }
  if (isVocabularyOutcome(outcome)) return { outcome: outcome as MessageOutcome, text: '' }
  return {
    outcome: 'possibly-pending',
    text: renderOutcome(action, 'possibly-pending', facts),
  }
}

async function resolveTarget(provider: MessageProvider, ref: string) {
  const target = ref === 'self' ? provider.caller().runId : ref
  let resolution
  try {
    resolution = await provider.resolve(target)
  } catch (err) {
    throw asToolError(err)
  }
  if (resolution.match === 'none') {
    const facts = { target: quote(ref) }
    throw new ToolError('unknown_target', repair('unknown_target', facts), facts)
  }
  if (resolution.match === 'ambiguous') {
    const candidates = resolution.candidates ?? []
    const facts = {
      target: quote(ref),
      count: String(candidates.length),
      candidates: quoteList(candidates),
    }
    throw new ToolError('ambiguous_name', repair('ambiguous_name', facts), facts)
  }
  return resolution
}

function resolutionLabel(resolution: { label?: string }, ref: string): string {
  return resolution.label || ref
}

async function tell(
  provider: MessageProvider,
  input: MessageInput,
  resolution: Awaited<ReturnType<typeof resolveTarget>>,
): Promise<ProviderResult> {
  const label = resolutionLabel(resolution, input.target ?? '')
  let result: ProviderResult
  try {
    result = await provider.tell(resolution.runId, input.message ?? '')
  } catch (err) {
    throw asToolError(err)
  }
  result.action = 'tell'
  result.label = label
  const rendered = outcomeText('tell', result.outcome, {
    message_id: quote(result.messageId ?? ''),
    target: quote(label),
  })
  result.outcome = rendered.outcome
  result.text = rendered.text
  return result
}

async function ask(
  provider: MessageProvider,
  input: MessageInput,
  resolution: Awaited<ReturnType<typeof resolveTarget>>,
  signal: AbortSignal | undefined,
): Promise<ProviderResult> {
  const label = resolutionLabel(resolution, input.target ?? '')
  let result: ProviderResult
  try {
    result = await provider.ask(resolution.runId, input.message ?? '', input.timeout_ms, signal)
  } catch (err) {
    throw asToolError(err)
  }
  result.action = 'ask'
  result.label = label
  const rendered = outcomeText('ask', result.outcome, {
    message_id: quote(result.messageId ?? ''),
    target: quote(label),
    reply: result.reply ?? '',
  })
  result.outcome = rendered.outcome
  result.text = rendered.text
  return result
}

async function resume(
  provider: MessageProvider,
  input: MessageInput,
  resolution: Awaited<ReturnType<typeof resolveTarget>>,
): Promise<ProviderResult> {
  let result: ProviderResult
  try {
    result = await provider.resume({
      target: resolution.runId,
      message: input.message ?? '',
      label: input.label,
      idempotencyKey: input.idempotency_key,
    })
  } catch (err) {
    throw asToolError(err)
  }
  result.action = 'resume'
  result.text = renderOutcome('resume', 'resumed', {
    run_id: quote(result.runId ?? ''),
    label: quote(result.label ?? ''),
  })
  try {
    await provider.registerResumed?.(result)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    throw new ToolError(
      'refused',
      `resume succeeded but registration failed for run_id ${quote(result.runId ?? '')} runtime_id ${quote(result.runtimeId ?? '')}: ${message}`,
    )
  }
  await provider.onResumed?.(result)
  return result
}

async function reply(provider: MessageProvider, input: MessageInput): Promise<ProviderResult> {
  const messageId = input.reply_to ?? ''
  const facts = { message_id: quote(messageId) }
  const edgeAbsent = (): ProviderResult => {
    const result: ProviderResult = { action: 'reply', outcome: 'edge-absent', messageId }
    result.text = renderOutcome('reply', result.outcome, facts)
    return result
  }
  if (!provider.claimAsk) return edgeAbsent()
  const asker = provider.claimAsk(messageId)
  if (asker === undefined) return edgeAbsent()
  let result: ProviderResult
  try {
    result = await provider.reply(messageId, input.message ?? '', asker)
  } catch (err) {
    provider.restoreAsk?.(messageId, asker)
    throw asToolError(err)
  }
  // Only a replied outcome proves the asker received the answer, so every other
  // outcome leaves the ask claimable.
  if (result.outcome !== 'replied') provider.restoreAsk?.(messageId, asker)
  result.action = 'reply'
  result.messageId = messageId
  const rendered = outcomeText('reply', result.outcome, facts)
  result.outcome = rendered.outcome
  result.text = rendered.text
  return result
}

async function withdraw(provider: MessageProvider, input: MessageInput): Promise<ProviderResult> {
  const messageId = input.message_id ?? ''
  let result: ProviderResult
  try {
    result = await provider.withdraw(messageId)
  } catch (err) {
    throw asToolError(err)
  }
  result.action = 'withdraw'
  result.messageId = messageId
  const rendered = outcomeText('withdraw', result.outcome, { message_id: quote(messageId) })
  result.outcome = rendered.outcome
  result.text = rendered.text
  return result
}

async function controlReply(provider: MessageProvider, input: MessageInput): Promise<ProviderResult> {
  if (!provider.controlReply) {
    throw new ToolError('unsupported_action', repair('control_reply_unsupported'))
  }
  let result: ProviderResult
  try {
    result = await provider.controlReply(input.target ?? '', input.payload ?? {})
  } catch (err) {
    throw asToolError(err)
  }
  result.action = 'control_reply'
  result.outcome = result.outcome ?? 'sent'
  result.text = renderOutcome('control_reply', result.outcome, {
    target: quote(input.target ?? ''),
  })
  return result
}

/** Route one avenor_message call through one provider. */
export async function executeMessage(
  provider: MessageProvider,
  input: unknown,
  options?: { signal?: AbortSignal },
): Promise<ProviderResult> {
  assertMessageInput(input)
  const typed = input
  const action = typed.action
  const conditions = routeConditions(provider)

  if (action === 'reply') return reply(provider, typed)
  if (action === 'withdraw') return withdraw(provider, typed)

  if (action === 'control_reply') {
    const denial = permissionDenial(action, conditions)
    if (denial) throw denial
    return controlReply(provider, typed)
  }

  const resolution = await resolveTarget(provider, typed.target ?? '')
  conditions.targetMatch = resolution.match
  const denial = permissionDenial(action, conditions)
  if (denial) throw denial
  let facts
  try {
    facts = await provider.state(resolution.runId)
  } catch (err) {
    throw refused(err)
  }
  conditions.targetLive = facts.live
  conditions.targetResumable = facts.resumable
  const stateDenied = stateDenial(action, conditions, resolution.runId, facts.status)
  if (stateDenied) throw stateDenied

  switch (action) {
    case 'tell':
      return tell(provider, typed, resolution)
    case 'ask':
      return ask(provider, typed, resolution, options?.signal)
    case 'resume':
      return resume(provider, typed, resolution)
  }
  throw invalidRequest(action, 'input is not a valid request object')
}

/** Build the peers `last` template facts: ids quoted, the stop reason sanitized. */
export function lastFacts(runId: string, label: string, stopReason: string): Record<string, string> {
  return { run_id: quote(runId), label: quote(label), stop_reason: sanitize(stopReason) }
}

/** Route one avenor_peers call through one provider. */
export async function executePeers(
  provider: MessageProvider,
  input: unknown,
): Promise<PeersRouterResult> {
  assertPeersInput(input)
  const typed = input
  const action = typed.action ?? 'list'

  if (action === 'list') {
    let peers
    try {
      peers = await provider.peers()
    } catch (err) {
      throw asToolError(err)
    }
    const result: PeersRouterResult = {
      action,
      peers: peers.peers,
      text: '',
    }
    if (peers.detail !== undefined) {
      result.outcome = 'lost'
      result.text = renderOutcome('peers_list', 'lost', { detail: sanitize(peers.detail) })
    }
    return result
  }

  if (action === 'rename') {
    let renamed
    try {
      renamed = await provider.rename({ sessionId: typed.session_id, name: typed.name ?? '' })
    } catch (err) {
      throw asToolError(err)
    }
    const runId = renamed.runId ?? ''
    const label = renamed.label ?? ''
    const previousLabel = renamed.previousLabel ?? ''
    return {
      action,
      outcome: 'renamed',
      peers: [],
      text: renderOutcome('peers_rename', 'renamed', {
        run_id: quote(runId),
        label: quote(label),
        previous_label: quote(previousLabel),
      }),
      renamed: { run_id: runId, label, previous_label: previousLabel },
    }
  }

  const resolution = await resolveTarget(provider, typed.target ?? '')
  let last
  try {
    last = await provider.last(resolution.runId)
  } catch (err) {
    throw asToolError(err)
  }
  const label = last.label || resolution.label || typed.target || ''
  const runId = resolution.publicRunId || resolution.runId
  const facts = lastFacts(runId, label, last.stopReason ?? '')
  let text: string
  if (!last.hasRecord) {
    text = renderLast('empty', facts)
  } else if (!last.finalOutput) {
    text = renderLast('no_text', facts)
  } else {
    text = `${renderLast('header', facts)}\n\n${last.finalOutput}`
    if (last.truncated) text += renderLast('truncated_suffix', facts)
  }
  return {
    action,
    peers: [],
    text,
    last: {
      run_id: runId,
      runtime_id: resolution.runId,
      label,
      final_output: last.finalOutput ?? '',
      stop_reason: last.stopReason ?? '',
      final_output_truncated: last.truncated ?? false,
    },
  }
}

/**
 * Render the peers list block the surfaces show. It is the one list text for
 * the shared records, so every TypeScript surface renders a peer identically.
 */
export function renderPeerList(records: PeerRecord[]): string {
  if (records.length === 0) return 'No other agent sessions connected.'
  const lines = records.map(record => {
    let line = `  ${record.run_id}`
    if (record.label) line = `  ${record.label} (${record.run_id})`
    const attrs = [record.kind, record.status, record.backend, record.model].filter(
      (value): value is string => typeof value === 'string' && value !== '',
    )
    if (attrs.length > 0) line += ` [${attrs.join(', ')}]`
    return line
  })
  return `**Active sessions:**\n${lines.join('\n')}`
}

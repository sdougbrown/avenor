// The one producer of messaging text for every TypeScript surface. The
// templates come from the shared schemas the Go renderers read, so the two
// languages cannot drift: `schemas/message.outcome.json` (outcome text and the
// peers `last` block), and `schemas/message_route.conformance.json` (repair
// sentences). Facts arrive pre-quoted, exactly as internal/messaging.Repair
// takes them.
import { randomBytes } from 'node:crypto'

import outcomeSchema from '../../../schemas/message.outcome.json'
import conformanceSchema from '../../../schemas/message_route.conformance.json'

interface OutcomeRow {
  outcome: string
  source: 'messaging' | 'tool'
  mayHaveBeenTransmitted: boolean
  terminal: boolean
  reuseMessageId: boolean
  fields: string[]
  render: Record<string, string>
}

interface OutcomeContract {
  outcomes: OutcomeRow[]
  matrix: Record<string, string[]>
  peers_last: Record<string, string>
}

const outcomeContract = outcomeSchema as unknown as OutcomeContract
const conformance = conformanceSchema as unknown as { repairs: Record<string, string> }

/** Every outcome the contract declares: the nine messaging rows and the two tool rows. */
export const MESSAGE_OUTCOMES = outcomeContract.outcomes.map(row => row.outcome) as [
  string,
  ...string[],
]

export type MessageOutcome = (typeof MESSAGE_OUTCOMES)[number]

/** The ask acceptance window the message ids encode, in milliseconds. */
export const MAX_WAIT_MS = 600_000

/** The bound `session_last_set` carries from the producer, in code points. */
export const MAX_FINAL_OUTPUT_RUNES = 4096

const outcomeRows: OutcomeRow[] = outcomeContract.outcomes
const renderTemplates: Record<string, string> = {}
const vocabulary = new Set<string>()
for (const row of outcomeRows) {
  if (row.source === 'messaging') vocabulary.add(row.outcome)
  for (const [action, template] of Object.entries(row.render)) {
    renderTemplates[`${action}:${row.outcome}`] = template
  }
}

const repairs: Record<string, string> = conformance.repairs
const peersLastTemplates: Record<string, string> = outcomeContract.peers_last

/** Substitute `{fact}` placeholders, leaving an unfilled placeholder intact. */
export function substitute(template: string | undefined, facts: Record<string, string> = {}): string {
  if (!template) return ''
  let out = ''
  for (let i = 0; i < template.length; ) {
    if (template[i] === '{') {
      const end = template.indexOf('}', i)
      if (end > i && Object.prototype.hasOwnProperty.call(facts, template.slice(i + 1, end))) {
        out += facts[template.slice(i + 1, end)]
        i = end + 1
        continue
      }
    }
    out += template[i]
    i++
  }
  return out
}

/** Render the (action, outcome) template; an unlisted pair renders as "". */
export function renderOutcome(
  action: string,
  outcome: string,
  facts: Record<string, string> = {},
): string {
  return substitute(renderTemplates[`${action}:${outcome}`], facts)
}

/** Report whether the contract has a template for the (action, outcome) pair. */
export function rendersOutcome(action: string, outcome: string): boolean {
  return Object.prototype.hasOwnProperty.call(renderTemplates, `${action}:${outcome}`)
}

/** Report whether the outcome is one the contract declares for messaging. */
export function isVocabularyOutcome(outcome: string): boolean {
  return vocabulary.has(outcome)
}

/** Render a repair sentence; the templates are the conformance fixture's `repairs`. */
export function repair(code: string, facts: Record<string, string> = {}): string {
  return substitute(repairs[code], facts)
}

/** Render one of the peers `last` templates. */
export function renderLast(key: string, facts: Record<string, string> = {}): string {
  return substitute(peersLastTemplates[key], facts)
}

/**
 * Render a value for a template fact as one JSON string literal. Control,
 * format, line- and paragraph-separator characters are escaped, so a label a
 * producer controls cannot carry a bidi override or a zero-width character into
 * tool text. The rule is byte-identical to internal/messaging.Quote.
 */
export function quote(value: string): string {
  return `"${escapeNonPrintable(value)}"`
}

/** Escape the same characters as quote without adding the surrounding quotes. */
export function sanitize(value: string): string {
  return escapeNonPrintable(value)
}

/** Render every value with quote and join them with ", ". */
export function quoteList(values: string[]): string {
  return values.map(quote).join(', ')
}

const NON_PRINTABLE = /[\p{Cf}\p{Zl}\p{Zp}]/u

function escapeNonPrintable(value: string): string {
  let out = ''
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    switch (character) {
      case '"':
        out += '\\"'
        continue
      case '\\':
        out += '\\\\'
        continue
      case '\b':
        out += '\\b'
        continue
      case '\f':
        out += '\\f'
        continue
      case '\n':
        out += '\\n'
        continue
      case '\r':
        out += '\\r'
        continue
      case '\t':
        out += '\\t'
        continue
    }
    if (code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f) || NON_PRINTABLE.test(character)) {
      if (code > 0xffff) {
        const high = 0xd800 + ((code - 0x10000) >> 10)
        const low = 0xdc00 + ((code - 0x10000) & 0x3ff)
        out += `\\u${high.toString(16).padStart(4, '0')}\\u${low.toString(16).padStart(4, '0')}`
      } else {
        out += `\\u${code.toString(16).padStart(4, '0')}`
      }
      continue
    }
    out += character
  }
  return out
}

/**
 * Mint a versioned message id whose acceptance deadline is encoded inside it.
 * The zero-deadline form spans the shared wait window, exactly like
 * messaging.NewMsgID(now, time.Time{}).
 */
export function newMsgID(now = Date.now()): string {
  const deadline = Math.trunc(now + MAX_WAIT_MS)
  return `av1.${deadline.toString(36)}.${randomBytes(16).toString('hex')}`
}

/**
 * Map pi's raw stop reasons onto avenor's canonical words, so a Pi host's
 * published reason and a Pi child's session.end reason agree. The table is
 * exactly internal/runtime/pi/events.go:normalizePiStopReason's; a spelling it
 * does not know passes through and stays unknown here too.
 */
export function normalizeTurnStopReason(stopReason: string): string {
  switch (stopReason) {
    case 'stop':
    case 'end_of_turn':
      return 'end_turn'
    case 'length':
      return 'max_tokens'
    case 'aborted':
      return 'cancelled'
    default:
      return stopReason
  }
}

function firstString(fields: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = fields[key]
    if (typeof value === 'string' && value !== '') return value
  }
  return ''
}

function extractPiText(value: unknown): string {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) {
    const parts: string[] = []
    for (const item of value) {
      const part = extractPiText(item)
      if (part !== '') parts.push(part)
    }
    return parts.join('')
  }
  if (value !== null && typeof value === 'object') {
    const fields = value as Record<string, unknown>
    for (const key of ['text', 'delta']) {
      const text = fields[key]
      if (typeof text === 'string' && text !== '') return text
    }
    for (const key of ['data', 'content', 'partial', 'assistantMessage', 'message']) {
      const text = extractPiText(fields[key])
      if (text !== '') return text
    }
  }
  return ''
}

/**
 * Read the last assistant message's text and raw stop reason from an agent_end
 * payload's `messages`, the TS twin of internal/runtime/pi's extractPiText plus
 * the per-message `stopReason` read that translateAgentEnd does. Go also falls
 * back to payload-level stopReason/errorMessage and normalises the stop reason;
 * this helper sees only the messages array and does neither.
 */
export function extractTurnText(messages: unknown[]): { text: string; stopReason: string } {
  if (messages.length === 0) return { text: '', stopReason: '' }
  const last = messages[messages.length - 1]
  if (last === null || typeof last !== 'object') return { text: '', stopReason: '' }
  const fields = last as Record<string, unknown>
  return {
    text: extractPiText(last),
    stopReason: firstString(fields, ['stopReason', 'stop_reason']),
  }
}

/** Bound a published turn at the same rune count the Go recorder uses. */
export function boundFinalOutput(text: string): string {
  const runes = Array.from(text)
  return runes.length <= MAX_FINAL_OUTPUT_RUNES
    ? text
    : runes.slice(0, MAX_FINAL_OUTPUT_RUNES).join('')
}

/** Extract the reply body from a wait result payload, as askclient.RenderReply does. */
export function extractReplyMessage(result: Record<string, unknown>): string {
  if (result.timeout === true) return '(timeout: no reply received)'
  if (result.cancelled === true) return '(cancelled)'
  const payload = result.payload
  if (payload && typeof payload === 'object') {
    const msg = (payload as Record<string, unknown>).message
    if (typeof msg === 'string' && msg) return msg
  }
  return JSON.stringify(result)
}

/** Extract the reply's own route key from a wait result payload, so the asker can answer the answer. */
export function extractReplyId(result: Record<string, unknown>): string {
  const payload = result.payload
  if (payload && typeof payload === 'object') {
    const id = (payload as Record<string, unknown>).id
    if (typeof id === 'string') return id
  }
  return ''
}

/**
 * Render the shared ask failure sentence for an ask that failed. The outcome
 * and the sentence both come from the contract, so TS and Go text agree.
 */
export function renderAskFailure(
  messageId: string,
  target: string,
  outcome: MessageOutcome = 'possibly-pending',
): string {
  return renderOutcome('ask', outcome, { message_id: quote(messageId), target: quote(target) })
}

/**
 * Reduce a caller-supplied ask timeout to what the broker accepts: a positive
 * finite number of milliseconds, or undefined to keep the broker's own wait.
 */
export function normalizeAskTimeoutMs(timeoutMs: number | undefined): number | undefined {
  return timeoutMs !== undefined && Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : undefined
}

/** Render a value the way Go's `%q` verb does (`strconv.Quote`). */
export function goQuote(value: string): string {
  let out = '"'
  for (const character of value) out += goEscapedRune(character)
  return `${out}"`
}

const GO_PRINTABLE = /[\p{L}\p{M}\p{N}\p{P}\p{S}]/u

/**
 * One rune in `strconv.Quote` form: short escapes for a quote, a backslash, a
 * bell and the whitespace controls; `\xNN` for the remaining ASCII escapes;
 * `\uNNNN` and `\UNNNNNNNN` for a non-printable rune; the rune itself when it
 * is printable. Printable is Go's `strconv.IsPrint` — ASCII space through `~`,
 * and elsewhere the categories L, M, N, P and S. V8's Unicode tables can be
 * newer than Go's, so a rune assigned since Go's tables were generated renders
 * escaped here where Go writes it verbatim; that direction only fails to match
 * a sentence, never invents one.
 */
function goEscapedRune(character: string): string {
  const code = character.codePointAt(0) ?? 0
  switch (character) {
    case '"':
      return '\\"'
    case '\\':
      return '\\\\'
    case '\x07':
      return '\\a'
    case '\b':
      return '\\b'
    case '\f':
      return '\\f'
    case '\n':
      return '\\n'
    case '\r':
      return '\\r'
    case '\t':
      return '\\t'
    case '\v':
      return '\\v'
  }
  const printable = code < 0x80 ? code >= 0x20 && code !== 0x7f : GO_PRINTABLE.test(character)
  if (printable) return character
  if (code < 0x80) return `\\x${code.toString(16).padStart(2, '0')}`
  if (code < 0x10000) return `\\u${code.toString(16).padStart(4, '0')}`
  return `\\U${code.toString(16).padStart(8, '0')}`
}

import type {
  ExtensionCommandContext,
  KeybindingsManager,
  Theme,
} from '@earendil-works/pi-coding-agent'
import type {
  Component,
  Focusable,
  TUI,
} from '@earendil-works/pi-tui'
import {
  Input,
  Key,
  matchesKey,
  truncateToWidth,
  visibleWidth,
  wrapTextWithAnsi,
} from '@earendil-works/pi-tui'
import type {
  InspectResult,
  RunObserver,
  RunSnapshot,
  RunToolState,
  RunTranscriptEntry,
  StatusResult,
} from '@dougbots/avenor-core'

const ANSI_PATTERN =
  // eslint-disable-next-line no-control-regex
  /[\u001B\u009B][[\]()#;?]*(?:(?:(?:[a-zA-Z\d]*(?:;[a-zA-Z\d]*)*)?\u0007)|(?:(?:\d{1,4}(?:;\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~]))/g

const TRANSCRIPT_SCROLL_STEP = 4
const ACTION_STATUS_CHARS = 180
const PREVIEW_CHARS = 240
const TOOL_PREVIEW_CHARS = 120
const FINAL_OUTPUT_CHARS = 600
const TRANSCRIPT_PADDING = 2
const INSPECT_ERROR_PREFIX = 'error: '

const TERMINAL_STATUSES = new Set(['done', 'failed', 'timeout', 'killed'])

export interface WatchRunRef {
  runId: string
  label?: string
  agent?: string
  runtimeId?: string
  supervisorId?: string
}

export interface WatchDeps {
  inspectRun(run: WatchRunRef): Promise<InspectResult>
  observeRun(
    run: WatchRunRef,
    afterSeq?: number,
    initialSnapshot?: InspectResult['snapshot'],
  ): Promise<RunObserver | null>
  cancelRun(run: WatchRunRef, runtimeId: string): Promise<void>
  interruptAndPromptRun(run: WatchRunRef, runtimeId: string, text: string): Promise<void>
  followUpRun(run: WatchRunRef, text: string): Promise<{ run_id: string; label: string }>
}

export interface WatchOpenOptions {
  trackedRuns: ReadonlyArray<WatchRunRef>
  deps: WatchDeps
  initialRunId?: string
  onTrackRun?: (run: WatchRunRef, status?: StatusResult) => void
}

export interface WatchViewState {
  run: WatchRunRef
  status?: StatusResult
  snapshot: RunSnapshot
  loading: boolean
  error?: string
  actionMessage?: string
}

function emptySnapshot(): RunSnapshot {
  return {
    identity: {},
    limits: {
      transcript_entries: 64,
      text_chars: 4000,
      tool_preview_chars: 1000,
      tools: 16,
      permissions: 16,
      legacy_keys: 128,
    },
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

export function sanitizeText(text: string): string {
  return text
    .replace(ANSI_PATTERN, '')
    .replaceAll('\t', '  ')
    .replace(/[\u0000-\u0008\u000b-\u001f\u007f]/g, '')
}

export function clipText(text: string | undefined, limit: number): string | undefined {
  if (!text) return undefined
  const clean = sanitizeText(text).trim()
  if (!clean) return undefined
  if (clean.length <= limit) return clean
  return `${clean.slice(0, Math.max(0, limit - 1))}…`
}

function compactWhitespace(text: string | undefined, limit: number): string | undefined {
  const clean = clipText(text, limit)
  if (!clean) return undefined
  return clean.replace(/\s+/g, ' ').trim() || undefined
}

function padLine(text: string, width: number): string {
  const clipped = truncateToWidth(text, width)
  return clipped + ' '.repeat(Math.max(0, width - visibleWidth(clipped)))
}

function wrapPrefixed(
  text: string | undefined,
  width: number,
  firstPrefix: string,
  nextPrefix = ' '.repeat(Math.max(0, visibleWidth(firstPrefix))),
): string[] {
  const clean = clipText(text, PREVIEW_CHARS)
  if (!clean) return []
  const bodyWidth = Math.max(10, width - Math.max(visibleWidth(firstPrefix), visibleWidth(nextPrefix)))
  const wrapped = wrapTextWithAnsi(clean, bodyWidth)
  return wrapped.map((line, index) =>
    truncateToWidth(`${index === 0 ? firstPrefix : nextPrefix}${line}`, width),
  )
}

function renderKv(theme: Theme, label: string, value: string | undefined, width: number): string[] {
  if (!value) return []
  return wrapPrefixed(value, width, theme.fg('dim', `${label}: `))
}

function usageSummary(usage: Record<string, unknown> | undefined): string | undefined {
  if (!usage) return undefined
  const order = ['input_tokens', 'output_tokens', 'total_tokens', 'tokens']
  const parts: string[] = []
  for (const key of order) {
    const value = usage[key]
    if (typeof value === 'number' && Number.isFinite(value)) {
      parts.push(`${key}=${value}`)
    }
  }
  if (parts.length === 0) {
    const entries = Object.entries(usage)
      .filter(([, value]) => typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean')
      .slice(0, 4)
      .map(([key, value]) => `${key}=${String(value)}`)
    if (entries.length === 0) return undefined
    return entries.join(' · ')
  }
  return parts.join(' · ')
}

export function finalOutputPreview(status: Pick<StatusResult, 'final_output'> | undefined, snapshot?: Pick<RunSnapshot, 'final_output' | 'assistant_text'>): string | undefined {
  return clipText(status?.final_output ?? snapshot?.final_output ?? snapshot?.assistant_text, FINAL_OUTPUT_CHARS)
}

function currentStatusWord(status: StatusResult | undefined, snapshot: RunSnapshot): string {
  const raw = status?.status?.toLowerCase()
  if (raw && TERMINAL_STATUSES.has(raw)) return raw
  if (snapshot.pending_permission) return 'waiting'
  if (snapshot.ended) {
    const stop = snapshot.stop_reason?.toLowerCase()
    if (stop && TERMINAL_STATUSES.has(stop)) return stop
    return raw && raw !== 'running' ? raw : 'done'
  }
  return raw ?? 'running'
}

function isPromptable(status: StatusResult | undefined, snapshot: RunSnapshot): boolean {
  const current = currentStatusWord(status, snapshot)
  return !TERMINAL_STATUSES.has(current) && current !== 'unavailable'
}

function isFollowUpable(status: StatusResult | undefined, snapshot: RunSnapshot): boolean {
  return TERMINAL_STATUSES.has(currentStatusWord(status, snapshot)) || snapshot.ended
}

function transcriptLabel(entry: RunTranscriptEntry, theme: Theme): string {
  switch (entry.kind) {
    case 'assistant':
      return theme.fg('text', 'A ')
    case 'thought':
      return theme.fg('muted', '~ ')
    case 'user':
      return theme.fg('accent', '> ')
    case 'tool':
      return theme.fg('toolTitle', `→ ${entry.title ?? 'tool'}${entry.status ? ` [${entry.status}]` : ''} `)
    case 'permission':
      return theme.fg('warning', '🔒 ')
    case 'session':
      return theme.fg('success', '■ ')
    case 'status':
      return theme.fg('dim', '· ')
  }
}

function transcriptText(entry: RunTranscriptEntry): string | undefined {
  if (entry.kind === 'tool') {
    return compactWhitespace(entry.text, TOOL_PREVIEW_CHARS) ?? entry.title
  }
  if (entry.kind === 'session') {
    return compactWhitespace(entry.text, PREVIEW_CHARS) ?? entry.status
  }
  return compactWhitespace(entry.text, PREVIEW_CHARS)
}

export function renderTranscriptLines(snapshot: RunSnapshot, width: number, theme: Theme): string[] {
  const out: string[] = []
  for (const entry of snapshot.transcript) {
    const text = transcriptText(entry)
    if (!text) continue
    const prefix = transcriptLabel(entry, theme)
    out.push(...wrapPrefixed(text, width, prefix))
  }
  if (snapshot.live_tools.length > 0) {
    if (out.length > 0) out.push('')
    out.push(theme.fg('dim', 'live tools:'))
    for (const tool of snapshot.live_tools) {
      out.push(...renderToolState(tool, width, theme))
    }
  }
  if (snapshot.pending_permission) {
    if (out.length > 0) out.push('')
    out.push(...wrapPrefixed(snapshot.pending_permission.description, width, theme.fg('warning', 'pending: ')))
  }
  return out
}

function renderToolState(tool: RunToolState, width: number, theme: Theme): string[] {
  const preview = compactWhitespace(tool.preview, TOOL_PREVIEW_CHARS)
  const prefix = theme.fg('toolTitle', `→ ${tool.title ?? tool.kind ?? 'tool'} `)
  const status = tool.live
    ? theme.fg('warning', '[running] ')
    : tool.status && `${theme.fg('dim', `[${tool.status}] `)}`
  const firstPrefix = `${prefix}${status ?? ''}`
  return wrapPrefixed(preview ?? '(no preview)', width, firstPrefix)
}

function helpText(state: WatchViewState): string {
  const actions: string[] = ['↑/↓ scroll', 'pgup/pgdn page', 'home/end jump', 'esc close']
  if (state.status && isPromptable(state.status, state.snapshot)) {
    actions.unshift('enter interrupt and send', 'ctrl-c cancel run')
  } else if (isFollowUpable(state.status, state.snapshot)) {
    actions.unshift('enter follow up')
  }
  return actions.join(' · ')
}

export class RunInspectorOverlay implements Component, Focusable {
  private readonly tui: TUI
  private readonly theme: Theme
  private readonly keybindings: KeybindingsManager
  private readonly deps: WatchDeps
  private readonly done: () => void
  private readonly onTrackRun?: (run: WatchRunRef, status?: StatusResult) => void

  private input = new Input()
  private state: WatchViewState
  private observer: RunObserver | null = null
  private observerUnsubscribe: (() => void) | null = null
  private refreshPending = false
  private scrollOffset = 0
  private closed = false
  private actionPending = false
  private actionError?: string

  private _focused = false
  get focused(): boolean {
    return this._focused
  }
  set focused(value: boolean) {
    this._focused = value
    this.input.focused = value
  }

  constructor(
    tui: TUI,
    theme: Theme,
    keybindings: KeybindingsManager,
    deps: WatchDeps,
    run: WatchRunRef,
    done: () => void,
    onTrackRun?: (run: WatchRunRef, status?: StatusResult) => void,
  ) {
    this.tui = tui
    this.theme = theme
    this.keybindings = keybindings
    this.deps = deps
    this.done = done
    this.onTrackRun = onTrackRun
    this.state = {
      run,
      snapshot: emptySnapshot(),
      loading: true,
    }
    this.input.onSubmit = (value: string) => {
      const text = value.trim()
      if (!text) return
      this.input.setValue('')
      this.submit(text)
    }
    void this.reload(true)
  }

  private requestRender(): void {
    this.tui.requestRender()
  }

  private close(): void {
    if (this.closed) return
    this.closed = true
    void this.cleanup()
    this.done()
  }

  dispose(): void {
    if (this.closed) return
    this.closed = true
    void this.cleanup()
  }

  invalidate(): void {
    this.input.invalidate()
  }

  private async cleanup(): Promise<void> {
    this.observerUnsubscribe?.()
    this.observerUnsubscribe = null
    if (this.observer) {
      const observer = this.observer
      this.observer = null
      await observer.close().catch(() => {})
    }
  }

  private async reload(withObserver: boolean): Promise<void> {
    this.state = { ...this.state, loading: true, error: undefined, actionMessage: undefined }
    this.requestRender()

    try {
      const result = await this.deps.inspectRun(this.state.run)
      if (this.closed) return

      const nextRun: WatchRunRef = {
        ...this.state.run,
        runId: result.status.run_id ?? this.state.run.runId,
        label: result.label ?? this.state.run.label,
        agent: result.status.agent ?? this.state.run.agent,
        runtimeId: result.status.runtime_id ?? result.snapshot.identity.runtime_id ?? this.state.run.runtimeId,
      }

      this.state = {
        run: nextRun,
        status: result.status,
        snapshot: result.snapshot,
        loading: false,
      }
      this.onTrackRun?.(nextRun, result.status)
      this.requestRender()

      await this.cleanup()

      if (!withObserver) return
      if (TERMINAL_STATUSES.has(currentStatusWord(result.status, result.snapshot))) return
      const runtimeId = nextRun.runtimeId ?? result.snapshot.identity.runtime_id
      if (!runtimeId) return

      const observer = await this.deps.observeRun(
        nextRun,
        result.snapshot.latest_seq,
        result.snapshot,
      )
      if (this.closed || !observer) {
        await observer?.close().catch(() => {})
        return
      }

      const applySnapshot = (snapshot: RunSnapshot) => {
        this.state = {
          ...this.state,
          snapshot: {
            ...snapshot,
            identity: {
              ...snapshot.identity,
              run_id: snapshot.identity.run_id ?? this.state.status?.run_id ?? this.state.run.runId,
            },
          },
        }
        if (snapshot.ended && !this.refreshPending) {
          this.refreshPending = true
          void this.reload(false).finally(() => {
            this.refreshPending = false
          })
        }
        this.requestRender()
      }

      this.observer = observer
      this.observerUnsubscribe = observer.subscribe((snapshot) => {
        applySnapshot(snapshot)
      })
      const current = observer.snapshot()
      if (current.last_event || current.transcript.length > 0 || current.pending_permission || current.ended) {
        applySnapshot(current)
      }
    } catch (error) {
      if (this.closed) return
      this.state = {
        ...this.state,
        loading: false,
        error: error instanceof Error ? error.message : String(error),
      }
      this.requestRender()
    }
  }

  private runAction(work: () => Promise<void>, message?: string): void {
    if (this.actionPending) return
    this.actionPending = true
    this.actionError = undefined
    if (message) {
      this.state = { ...this.state, actionMessage: clipText(message, ACTION_STATUS_CHARS) }
    }
    this.requestRender()
    void work()
      .catch((error) => {
        this.actionError = error instanceof Error ? error.message : String(error)
      })
      .finally(() => {
        this.actionPending = false
        this.requestRender()
      })
  }

  private submit(text: string): void {
    const runtimeId = this.state.run.runtimeId ?? this.state.status?.runtime_id ?? this.state.snapshot.identity.runtime_id
    if (this.state.status && isPromptable(this.state.status, this.state.snapshot) && runtimeId) {
      this.runAction(async () => {
        await this.deps.interruptAndPromptRun(this.state.run, runtimeId, text)
        this.state = { ...this.state, actionMessage: clipText(`Interrupted and sent: ${text}`, ACTION_STATUS_CHARS) }
      }, 'interrupting and sending…')
      return
    }

    if (!isFollowUpable(this.state.status, this.state.snapshot)) return

    this.runAction(async () => {
      const next = await this.deps.followUpRun(this.state.run, text)
      this.state = {
        ...this.state,
        run: {
          ...this.state.run,
          runId: next.run_id,
          label: next.label,
          runtimeId: undefined,
        },
        actionMessage: clipText(`Started follow-up run ${next.label}`, ACTION_STATUS_CHARS),
      }
      this.scrollOffset = 0
      await this.reload(true)
    }, 'starting follow-up…')
  }

  private cancelCurrentRun(): void {
    const runtimeId = this.state.run.runtimeId ?? this.state.status?.runtime_id ?? this.state.snapshot.identity.runtime_id
    if (!runtimeId) return
    this.runAction(async () => {
      await this.deps.cancelRun(this.state.run, runtimeId)
      this.state = { ...this.state, actionMessage: 'cancel requested' }
    }, 'cancelling run…')
  }

  handleInput(data: string): void {
    if (matchesKey(data, Key.escape)) {
      this.close()
      return
    }
    if (matchesKey(data, Key.up)) {
      this.scrollOffset += TRANSCRIPT_SCROLL_STEP
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.down)) {
      this.scrollOffset = Math.max(0, this.scrollOffset - TRANSCRIPT_SCROLL_STEP)
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.pageUp)) {
      this.scrollOffset += this.viewportHeight()
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.pageDown)) {
      this.scrollOffset = Math.max(0, this.scrollOffset - this.viewportHeight())
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.home)) {
      this.scrollOffset = Number.MAX_SAFE_INTEGER
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.end)) {
      this.scrollOffset = 0
      this.requestRender()
      return
    }
    if (matchesKey(data, Key.ctrl('c')) && this.state.status && isPromptable(this.state.status, this.state.snapshot)) {
      this.cancelCurrentRun()
      return
    }

    this.input.handleInput(data)
    this.requestRender()
  }

  private viewportHeight(): number {
    const rows = this.tui.terminal.rows || 30
    return Math.max(8, rows - 15)
  }

  render(width: number): string[] {
    const theme = this.theme
    const lines: string[] = []
    const horizontal = theme.fg('borderAccent', '─'.repeat(Math.max(1, width)))
    const innerWidth = Math.max(10, width)

    lines.push(horizontal)

    const title = this.state.run.label ?? this.state.run.runId
    const statusWord = currentStatusWord(this.state.status, this.state.snapshot)
    const heading = `${theme.fg('accent', theme.bold(title))} ${theme.fg('dim', `(${this.state.run.runId})`)} ${theme.fg(statusWord === 'done' ? 'success' : statusWord === 'failed' ? 'error' : statusWord === 'waiting' ? 'warning' : 'muted', statusWord)}`
    lines.push(truncateToWidth(heading, innerWidth))

    const phase = this.state.snapshot.phase_label ?? this.state.status?.phase_label ?? this.state.snapshot.phase ?? this.state.status?.phase
    const summary = [
      phase ? `phase=${phase}` : undefined,
      this.state.snapshot.latest_seq !== undefined ? `seq=${this.state.snapshot.latest_seq}` : undefined,
      this.state.snapshot.identity.agent ?? this.state.status?.agent,
      this.state.snapshot.identity.model ?? this.state.status?.model,
      this.state.snapshot.identity.backend ?? this.state.status?.backend,
    ].filter((value): value is string => !!value).join(' · ')
    lines.push(...renderKv(theme, 'status', summary || undefined, innerWidth))

    const identity = [
      this.state.snapshot.identity.runtime_id ?? this.state.status?.runtime_id,
      this.state.snapshot.identity.session_id ?? this.state.status?.session_id,
      this.state.snapshot.identity.dir ?? this.state.status?.dir,
    ].filter((value): value is string => !!value).join(' · ')
    lines.push(...renderKv(theme, 'identity', identity || undefined, innerWidth))

    lines.push(...renderKv(theme, 'usage', usageSummary(this.state.snapshot.usage ?? this.state.status?.usage), innerWidth))

    const finalOutput = finalOutputPreview(this.state.status, this.state.snapshot)
    if (finalOutput && (this.state.snapshot.ended || TERMINAL_STATUSES.has(statusWord))) {
      lines.push(...wrapPrefixed(finalOutput, innerWidth, theme.fg('success', 'final: ')))
    }

    if (this.state.snapshot.pending_permission) {
      lines.push(...wrapPrefixed(this.state.snapshot.pending_permission.description, innerWidth, theme.fg('warning', 'permission: ')))
      const optionText = this.state.snapshot.pending_permission.options
        .map((option) => `${option.option_id} (${option.label})`)
        .join(', ')
      lines.push(...wrapPrefixed(optionText, innerWidth, theme.fg('dim', 'options: ')))
    } else if (this.state.status?.pending_permission) {
      lines.push(...wrapPrefixed(this.state.status.pending_permission.description, innerWidth, theme.fg('warning', 'permission: ')))
      const optionText = this.state.status.pending_permission.options
        .map((option) => `${option.option_id} (${option.label})`)
        .join(', ')
      lines.push(...wrapPrefixed(optionText, innerWidth, theme.fg('dim', 'options: ')))
    }

    const liveTools = this.state.snapshot.live_tools.slice(-4)
    if (liveTools.length > 0) {
      lines.push(theme.fg('dim', 'live tools:'))
      for (const tool of liveTools) {
        lines.push(...renderToolState(tool, innerWidth, theme))
      }
    }

    if (this.state.loading) {
      lines.push(theme.fg('muted', 'loading…'))
    }
    if (this.state.actionMessage) {
      lines.push(...wrapPrefixed(this.state.actionMessage, innerWidth, theme.fg('accent', 'action: ')))
    }
    if (this.actionError) {
      lines.push(...wrapPrefixed(this.actionError, innerWidth, theme.fg('error', INSPECT_ERROR_PREFIX)))
    } else if (this.state.error) {
      lines.push(...wrapPrefixed(this.state.error, innerWidth, theme.fg('error', INSPECT_ERROR_PREFIX)))
    }

    lines.push(horizontal)
    lines.push(theme.fg('dim', 'transcript:'))

    const transcript = renderTranscriptLines(this.state.snapshot, Math.max(10, innerWidth - TRANSCRIPT_PADDING), theme)
      .map(line => truncateToWidth(`  ${line}`, innerWidth))
    const viewportHeight = this.viewportHeight()
    const scrollNotice = this.scrollOffset > 0
      ? truncateToWidth(theme.fg('dim', `… ${this.scrollOffset} line${this.scrollOffset === 1 ? '' : 's'} below`), innerWidth)
      : undefined
    const transcriptCapacity = Math.max(1, viewportHeight - (scrollNotice ? 1 : 0))
    const maxOffset = Math.max(0, transcript.length - transcriptCapacity)
    if (this.scrollOffset > maxOffset) {
      this.scrollOffset = maxOffset
    }
    const end = transcript.length - this.scrollOffset
    const start = Math.max(0, end - transcriptCapacity)
    const body = transcript.slice(start, end)
    while (body.length < transcriptCapacity) {
      body.push('')
    }
    if (scrollNotice) {
      body.push(scrollNotice)
    }
    for (const line of body) {
      lines.push(padLine(line, innerWidth))
    }

    lines.push(horizontal)
    lines.push(...this.input.render(innerWidth).map(line => truncateToWidth(line, innerWidth)))

    const promptHint = this.state.status && isPromptable(this.state.status, this.state.snapshot)
      ? 'interrupt and send to running/waiting run'
      : isFollowUpable(this.state.status, this.state.snapshot)
        ? 'start a follow-up run from this transcript'
        : 'read-only view'
    lines.push(truncateToWidth(theme.fg('dim', `${promptHint} · ${helpText(this.state)}`), innerWidth))
    lines.push(horizontal)

    return lines.map(line => truncateToWidth(line, innerWidth))
  }
}

function matchingWatchRuns(
  runs: ReadonlyArray<WatchRunRef>,
  selector: string,
): WatchRunRef[] {
  const query = selector.trim().toLowerCase()
  if (!query) return []
  // A run id is only unique within a supervisor; custom supervisors may reuse
  // ids (e.g. rt_1), so surface every match and let the picker disambiguate.
  const byId = runs.filter(run => run.runId.toLowerCase() === query)
  if (byId.length > 0) return byId
  return runs.filter(run =>
    run.label?.toLowerCase() === query || run.agent?.toLowerCase() === query,
  )
}

function watchRunChoice(run: WatchRunRef): string {
  const label = sanitizeText(run.label ?? run.runId)
  const agent = run.agent ? ` · ${sanitizeText(run.agent)}` : ''
  const supervisor = run.supervisorId ? ` · ${sanitizeText(run.supervisorId)}` : ''
  return `${label}${agent}${supervisor} · ${sanitizeText(run.runId).slice(0, 8)}`
}

async function selectWatchRun(
  ctx: ExtensionCommandContext,
  runs: ReadonlyArray<WatchRunRef>,
): Promise<WatchRunRef | undefined> {
  const choices = runs.map(watchRunChoice)
  const selected = await ctx.ui.select('Watch run:', choices)
  if (!selected) return undefined
  return runs.find((_run, index) => choices[index] === selected)
}

export async function openRunInspector(
  ctx: ExtensionCommandContext,
  options: WatchOpenOptions,
): Promise<void> {
  if (!ctx.hasUI) {
    ctx.ui.notify('avenor-watch requires TUI mode', 'warning')
    return
  }

  const selector = options.initialRunId?.trim()
  let tracked: WatchRunRef | undefined
  let runId: string
  if (selector) {
    const matches = matchingWatchRuns(options.trackedRuns, selector)
    tracked = matches.length === 1
      ? matches[0]
      : matches.length > 1
        ? await selectWatchRun(ctx, matches)
        : undefined
    if (matches.length > 1 && !tracked) return
    runId = tracked?.runId ?? selector
  } else {
    if (options.trackedRuns.length === 0) {
      ctx.ui.notify('No tracked runs', 'warning')
      return
    }
    tracked = await selectWatchRun(ctx, options.trackedRuns)
    if (!tracked) return
    runId = tracked.runId
  }

  await ctx.ui.custom<void>(
    (tui, theme, keybindings, done) => new RunInspectorOverlay(
      tui,
      theme,
      keybindings,
      options.deps,
      tracked ?? { runId },
      () => done(),
      options.onTrackRun,
    ),
    {
      overlay: true,
      overlayOptions: {
        anchor: 'center',
        width: '95%',
        maxHeight: '95%',
        minWidth: 80,
      },
    },
  )
}

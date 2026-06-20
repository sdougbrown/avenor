import type { Component } from '@earendil-works/pi-tui'

export interface WatchEvent {
  type: string
  ts: number
  [key: string]: unknown
}

export class EventFeedOverlay implements Component {
  private _events: WatchEvent[] = []
  private _runId: string
  private _onClose: () => void
  private _maxLines = 50

  constructor(runId: string, onClose?: () => void) {
    this._runId = runId
    this._onClose = onClose ?? (() => {})
  }

  setOnClose(fn: () => void): void {
    this._onClose = fn
  }

  setEvents(events: WatchEvent[]): void {
    this._events = events.slice(-this._maxLines)
  }

  appendEvent(event: WatchEvent): void {
    this._events.push(event)
    if (this._events.length > this._maxLines) {
      this._events = this._events.slice(-this._maxLines)
    }
  }

  render(width: number): string[] {
    const lines: string[] = []
    const header = `📡 avenor watch: ${this._runId.slice(0, 8)}… (Esc to close)`
    lines.push(header)
    lines.push('─'.repeat(Math.min(width, header.length)))

    if (this._events.length === 0) {
      lines.push('  (waiting for events…)')
    } else {
      for (const evt of this._events) {
        const ts = new Date(evt.ts).toISOString().slice(11, 19)
        const type = evt.type ?? 'unknown'
        const extra = this.summarizeEvent(evt)
        lines.push(`  ${ts} ${type}${extra ? ` — ${extra}` : ''}`)
      }
    }

    return lines
  }

  handleInput(data: string): void {
    if (data === '\x1b') {
      this._onClose()
    }
  }

  private summarizeEvent(evt: WatchEvent): string {
    if (evt.type === 'tool.call' || evt.type === 'tool.use') {
      return `called ${String(evt.tool ?? evt.name ?? '')}`
    }
    if (evt.type === 'agent.message_chunk') return 'thinking…'
    if (evt.type?.startsWith('permission.')) return 'permission requested'
    if (evt.type === 'tool.result') {
      const result = evt.result as string | undefined
      if (typeof result === 'string' && result.length < 80) return result
      return 'completed'
    }
    return ''
  }

  invalidate(): void {}
}

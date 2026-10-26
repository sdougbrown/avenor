import type { TerminalStatus } from './types.js'

/** One terminal completion waiting for a safe model-facing delivery. */
export interface CompletionNotice {
  /** Canonical supervisor + public run id key. */
  key: string
  runId: string
  supervisorId?: string
  runtimeId?: string
  label: string
  agent?: string
  status: TerminalStatus
  text: string
}

/** Unread terminal completions, keyed canonically so polls and concurrent
 * result calls cannot enqueue or consume the same run twice. */
export class CompletionQueue {
  private readonly notices = new Map<string, CompletionNotice>()

  get size(): number {
    return this.notices.size
  }

  has(key: string): boolean {
    return this.notices.has(key)
  }

  set(notice: CompletionNotice): void {
    this.notices.set(notice.key, notice)
  }

  delete(key: string): boolean {
    return this.notices.delete(key)
  }

  entries(): CompletionNotice[] {
    return [...this.notices.values()]
  }

  clear(): void {
    this.notices.clear()
  }
}

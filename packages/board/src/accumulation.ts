const CHUNK_TYPES = new Set(['agent.thought_chunk', 'agent.message_chunk', 'user.message_chunk'])

export function isChunkEvent(type: string) {
  return CHUNK_TYPES.has(type)
}

export function extractChunkText(data: unknown): string {
  if (typeof data === 'object' && data !== null) {
    const d = data as Record<string, unknown>
    if (typeof d.content === 'string' && d.content.length > 0) return d.content
    if (typeof d.text === 'string' && d.text.length > 0) return d.text
  }
  return ''
}

export interface AccumulatorEntry {
  type: string
  content: string
  timestamp: number
}

export class Accumulator {
  private accType = ''
  private accText = ''
  private accTimestamp = 0

  flush(): AccumulatorEntry | null {
    if (this.accType && this.accText) {
      const content = this.accText.replace(/\s+/g, ' ').trim()
      const type = this.accType
      const timestamp = this.accTimestamp
      this.accType = ''
      this.accText = ''
      this.accTimestamp = 0
      return { type, content, timestamp }
    }
    return null
  }

  ingest(type: string, data: unknown, timestamp: number): AccumulatorEntry | null {
    if (!isChunkEvent(type)) {
      return this.flush()
    }
    const text = extractChunkText(data)
    if (!text) return null
    if (this.accType !== type) {
      // Flush previous buffer if any before starting new one
      return this.flushAndStart(type, timestamp, text)
    }
    this.accText += text
    return null
  }

  private flushAndStart(type: string, timestamp: number, text: string): AccumulatorEntry | null {
    const flushed = this.flush()
    this.accType = type
    this.accTimestamp = timestamp
    this.accText = text
    return flushed
  }

  get isBuffered(): boolean {
    return this.accType !== ''
  }
}

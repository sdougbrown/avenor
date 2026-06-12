import {
  createSignal,
  createEffect,
  onMount,
  Show,
} from 'solid-js'
import { createEventSource, fetchConfig } from './api'
import { ControlBar } from './ControlBar'
import { Accumulator, isChunkEvent } from './accumulation'

interface Props {
  socketPath: string | null
  runtimeId: string | undefined
}

interface EventItem {
  type: string
  data: unknown
  timestamp: number
}

export function EventFeed(props: Props) {
  const [events, setEvents] = createSignal<EventItem[]>([])
  const [autoScroll, setAutoScroll] = createSignal(true)
  const [mutationsAllowed, setMutationsAllowed] = createSignal(false)
  const [expandedTools, setExpandedTools] = createSignal<Set<number>>(new Set())

  const accumulator = new Accumulator()

  // Config
  onMount(() => {
    fetchConfig()
      .then((cfg) => setMutationsAllowed(!!cfg.mutations_allowed))
      .catch(() => {})
  })

  function pushEvent(type: string, content: string, timestamp: number) {
    setEvents((prev) => {
      const next = [...prev, { type, data: { content }, timestamp }]
      if (next.length > 10000) {
        return next.slice(next.length - 10000)
      }
      return next
    })
  }

  function flushAccumulation() {
    const entry = accumulator.flush()
    if (entry) {
      pushEvent(entry.type, entry.content, entry.timestamp)
    }
  }

  // EventSource lifecycle
  createEffect(() => {
    const path = props.socketPath
    if (!path) return

    const source = createEventSource(path, props.runtimeId)

    source.addEventListener('event', (e: MessageEvent) => {
      try {
        const parsed = JSON.parse(e.data)
        const eventType = parsed.event || parsed.type || 'unknown'

        const flushed = accumulator.ingest(eventType, parsed, Date.now())
        if (flushed) {
          pushEvent(flushed.type, flushed.content, flushed.timestamp)
        }
        if (!isChunkEvent(eventType)) {
          flushAccumulation()
        }
      } catch {
        flushAccumulation()
        setEvents((prev) => [
          ...prev,
          { type: 'unknown', data: e.data, timestamp: Date.now() },
        ])
      }
    })

    source.onerror = () => {
      source.close()
    }

    return () => {
      flushAccumulation()
      source.close()
    }
  })

  // Auto-scroll tracking
  onMount(() => {
    const stream = document.getElementById('event-stream')
    if (!stream) return

    const onScroll = () => {
      const atBottom =
        stream.scrollHeight - stream.scrollTop - stream.clientHeight < 50
      setAutoScroll(atBottom)
    }
    stream.addEventListener('scroll', onScroll)
    return () => stream.removeEventListener('scroll', onScroll)
  })

  // Auto-scroll: jump when new events arrive
  createEffect(() => {
    if (!autoScroll()) return
    if (events().length === 0) return
    const stream = document.getElementById('event-stream')
    if (!stream) return
    stream.scrollTop = stream.scrollHeight
  })

  function scrollToBottom() {
    setAutoScroll(true)
    const stream = document.getElementById('event-stream')
    if (!stream) return
    stream.scrollTop = stream.scrollHeight
  }

  function toggleTool(idx: number) {
    setExpandedTools((prev) => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }

  // Event rendering
  function renderEvent(ev: EventItem, idx: number) {
    const ts = new Date(ev.timestamp).toLocaleTimeString()

    switch (ev.type) {
      case 'agent.thought_chunk':
      case 'agent.message_chunk':
      case 'user.message_chunk': {
        const content = (ev.data as Record<string, unknown>)?.content as string | undefined
        const label = ev.type === 'agent.thought_chunk' ? 'thought'
          : ev.type === 'agent.message_chunk' ? 'message'
          : 'user-message'
        return (
          <div class="event event-thought">
            <span class="event-time">{ts}</span>
            <span class="event-type">{label}</span>
            <pre>{content || ''}</pre>
          </div>
        )
      }
      case 'tool.call': {
        const d = ev.data as Record<string, unknown>
        const toolName = (d.tool_name as string) || 'tool'
        const isExpanded = expandedTools().has(idx)
        return (
          <div class="event event-tool">
            <span class="event-time">{ts}</span>
            <span class="event-type">tool</span>
            <button class="tool-toggle" onClick={() => toggleTool(idx)}>
              {toolName} → {isExpanded ? 'collapse' : 'expand'}
            </button>
            {isExpanded && (
              <pre class="tool-details">
                {JSON.stringify(ev.data, null, 2)}
              </pre>
            )}
          </div>
        )
      }
      case 'agent.status': {
        const d = ev.data as Record<string, unknown>
        const phase = (d.phase || d.phase_label || 'status') as string
        return (
          <div class="event event-status">
            <span class="event-time">{ts}</span>
            <span class="phase-badge">{phase}</span>
          </div>
        )
      }
      case 'session.start':
        return (
          <div class="event event-divider">── session.start ──</div>
        )
      case 'session.end':
        return (
          <div class="event event-divider">── session.end ──</div>
        )
      case 'permission.request': {
        const perm = ev.data as Record<string, unknown>
        return (
          <div class="event event-permission">
            <span class="event-time">{ts}</span>
            <span class="perm-label">⚠ permission.request</span>
            <pre>{JSON.stringify(perm, null, 2)}</pre>
          </div>
        )
      }
      default:
        return (
          <div class="event event-unknown">
            <span class="event-time">{ts}</span>
            <span class="event-type">{ev.type}</span>
            <pre>{JSON.stringify(ev.data, null, 2)}</pre>
          </div>
        )
    }
  }

  // Layout
  return (
    <main class="feed">
      <div id="event-stream" class="event-stream">
        {!props.socketPath && (
          <div class="feed-placeholder">Select a socket from the sidebar</div>
        )}
        {props.socketPath && events().length === 0 && (
          <div class="feed-placeholder">Waiting for events…</div>
        )}
        {events().map((ev, i) => renderEvent(ev, i))}
      </div>
      {autoScroll() === false && events().length > 10 && (
        <button class="scroll-bottom" onClick={scrollToBottom}>
          ↓ Scroll to bottom
        </button>
      )}
      <Show when={mutationsAllowed() && !!props.socketPath}>
        <ControlBar
          socketPath={props.socketPath!}
          runtimeId={props.runtimeId}
        />
      </Show>
    </main>
  )
}

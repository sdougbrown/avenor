import { createSignal, Show, For } from 'solid-js'
import type { SocketInfo, RuntimeInfo } from './api'
import { fetchRuntimes } from './api'

interface Props {
  sockets: SocketInfo[]
  selected: string | null
  onSelect: (path: string, runtimeId?: string) => void
  onRuntimeSelect: (path: string, id: string) => void
}

export function Sidebar(props: Props) {
  const [expanded, setExpanded] = createSignal<Set<string>>(new Set())
  const [cache, setCache] = createSignal<Record<string, RuntimeInfo[]>>({})

  function toggleExpand(path: string) {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(path)) next.delete(path)
      else next.add(path)
      return next
    })
    // Load runtimes if not cached
    const socket = props.sockets.find((s) => s.path === path)
    if (socket && socket.type === 'stable' && !cache()[path]) {
      fetchRuntimes(path).then((r) =>
        setCache((prev) => ({ ...prev, [path]: r }))
      ).catch(() => {})
    }
  }

  function onSocketClick(socket: SocketInfo) {
    if (socket.type === 'stable') {
      toggleExpand(socket.path)
    }
    props.onSelect(socket.path)
  }

  function timeAgo(ts: number): string {
    const secs = (Date.now() - ts) / 1000
    if (secs < 5) return 'now'
    if (secs < 60) return `${Math.floor(secs)}s`
    if (secs < 3600) return `${Math.floor(secs / 60)}m`
    return `${Math.floor(secs / 3600)}h`
  }

  return (
    <nav class="sidebar">
      <div class="sidebar-title">Avenor</div>
      {props.sockets.length === 0 && (
        <div class="sidebar-empty">No sockets found</div>
      )}
      <ul class="socket-list">
        <For each={props.sockets}>
          {(socket: SocketInfo) => (
            <li class="socket-item">
              <div
                classList={{ 'socket-row': true, selected: props.selected === socket.path }}
                onClick={() => onSocketClick(socket)}
              >
                <span
                  classList={{
                    'status-dot': true,
                    online: !!socket.status,
                    offline: !socket.status,
                  }}
                />
                <span class="socket-path">{socket.path}</span>
                <span class="socket-type">{socket.type}</span>
                {socket.status && (
                  <span class="socket-time">{timeAgo(socket.status.updated_at)}</span>
                )}
              </div>
              <Show when={socket.type === 'stable' && expanded().has(socket.path)}>
                <ul class="runtime-list">
                  <For each={cache()[socket.path] || []}>
                    {(r: RuntimeInfo) => (
                      <li
                        class="runtime-item"
                        onClick={() => props.onRuntimeSelect(socket.path, r.runtime_id)}
                      >
                        <span class="runtime-label">
                          {r.label || r.session_id}
                        </span>
                        <Show when={r.turn_state}>
                          <span class="runtime-turn">{String(r.turn_state)}</span>
                        </Show>
                      </li>
                    )}
                  </For>
                </ul>
              </Show>
              <Show when={socket.type === 'stable' && !expanded().has(socket.path)}>
                <button
                  class="expand-btn"
                  onClick={(e) => {
                    e.stopPropagation()
                    toggleExpand(socket.path)
                  }}
                >
                  + expand
                </button>
              </Show>
            </li>
          )}
        </For>
      </ul>
    </nav>
  )
}

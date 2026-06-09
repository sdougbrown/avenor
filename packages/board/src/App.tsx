import { createSignal, onMount } from 'solid-js'
import type { SocketInfo } from './api'
import { fetchSockets } from './api'
import { Sidebar } from './Sidebar'
import { EventFeed } from './EventFeed'
import './styles.css'

export function App() {
  const [sockets, setSockets] = createSignal<SocketInfo[]>([])
  const [selectedPath, setSelectedPath] = createSignal<string | null>(null)
  const [selectedRuntimeId, setSelectedRuntimeId] = createSignal<string | undefined>(undefined)

  onMount(() => {
    refresh()
    const id = setInterval(refresh, 5000)
    return () => clearInterval(id)
  })

  async function refresh() {
    try {
      setSockets(await fetchSockets())
    } catch {
      /* ignore */
    }
  }

  function onSelect(path: string, runtimeId?: string) {
    setSelectedPath(path)
    setSelectedRuntimeId(runtimeId)
  }

  return (
    <div class="app">
      <Sidebar
        sockets={sockets()}
        selected={selectedPath()}
        onSelect={onSelect}
        onRuntimeSelect={(path, id) => {
          setSelectedPath(path)
          setSelectedRuntimeId(id)
        }}
      />
      <EventFeed
        socketPath={selectedPath()}
        runtimeId={selectedRuntimeId()}
      />
    </div>
  )
}

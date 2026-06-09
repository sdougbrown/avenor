import { createSignal } from 'solid-js'
import { sendPrompt, cancelRun } from './api'

interface ControlBarProps {
  socketPath: string
  runtimeId?: string
}

export function ControlBar(props: ControlBarProps) {
  const [text, setText] = createSignal('')
  const [loading, setLoading] = createSignal(false)

  async function handleSend() {
    const t = text().trim()
    if (!t) return
    setLoading(true)
    try {
      await sendPrompt(props.socketPath, t, props.runtimeId)
      setText('')
    } finally {
      setLoading(false)
    }
  }

  async function handleCancel() {
    setLoading(true)
    try {
      await cancelRun(props.socketPath, props.runtimeId)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div class="control-bar">
      <input
        class="prompt-input"
        placeholder="Type a prompt…"
        value={text()}
        onInput={(e) => setText(e.currentTarget.value)}
        onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) handleSend() }}
        disabled={loading()}
      />
      <button class="btn btn-send" onClick={handleSend} disabled={loading() || !text().trim()}>
        Send
      </button>
      <button class="btn btn-cancel" onClick={handleCancel} disabled={loading()}>
        Cancel
      </button>
    </div>
  )
}

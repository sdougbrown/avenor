// ── Types ──────────────────────────────────────────────────────────────────

export interface SocketInfo {
  path: string
  type: 'one-shot' | 'stable'
  pid: number
  status?: RunStatus
}

export interface RunStatus {
  session_id?: string
  run_id?: string
  run_label?: string
  phase?: string
  phase_label?: string
  last_event?: string
  turn_state?: string
  retry_attempt?: number
  max_retries?: number
  pending_permission: boolean
  permission?: Record<string, unknown>
  started_at: number
  updated_at: number
}

export interface RuntimeInfo {
  runtime_id: string
  label: string
  session_id: string
  status: string
  [key: string]: unknown
}

export interface AuthConfig {
  mutations_allowed: boolean
  auth_required: boolean
}

// ── Helpers ────────────────────────────────────────────────────────────────

// Auto-capture ?token= from URL into localStorage, then strip it from the
// URL bar so the token doesn't linger in browser history or address bar.
const urlParams = new URLSearchParams(window.location.search)
const urlToken = urlParams.get('token')
if (urlToken) {
  localStorage.setItem('avenor_auth_token', urlToken)
  urlParams.delete('token')
  const clean = urlParams.toString()
  const newPath = window.location.pathname + (clean ? `?${clean}` : '')
  window.history.replaceState({}, '', newPath)
}

function authHeader(): Record<string, string> {
  const token = localStorage.getItem('avenor_auth_token')
  if (!token) return {}
  return { Authorization: `Bearer ${token}` }
}

// ── API functions ──────────────────────────────────────────────────────────

export async function fetchSockets(): Promise<SocketInfo[]> {
  const res = await fetch('/api/sockets')
  return res.json()
}

export async function fetchStatus(
  socketPath: string,
): Promise<RunStatus> {
  const res = await fetch(`/api/status?socket=${encodeURIComponent(socketPath)}`)
  return res.json()
}

export async function fetchRuntimes(
  socketPath: string,
): Promise<RuntimeInfo[]> {
  const res = await fetch(
    `/api/list?socket=${encodeURIComponent(socketPath)}`,
  )
  return res.json()
}

export function createEventSource(
  socketPath: string,
  runtimeId?: string,
): EventSource {
  let url = `/api/events?socket=${encodeURIComponent(socketPath)}${
    runtimeId ? `&runtime_id=${encodeURIComponent(runtimeId)}` : ''
  }`
  // EventSource cannot set custom headers, so pass token as query param.
  // This leaks the token into browser history and server logs — acceptable
  // because the board only binds loopback by default and the token is
  // randomly generated (or user-provided on a trusted network).
  const token = localStorage.getItem('avenor_auth_token')
  if (token) {
    url += `&token=${encodeURIComponent(token)}`
  }
  return new EventSource(url)
}

export async function cancelRun(
  socketPath: string,
  runtimeId?: string,
): Promise<{ ok: boolean }> {
  const res = await fetch('/api/cancel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeader() },
    body: JSON.stringify({
      socket: socketPath,
      ...(runtimeId ? { runtime_id: runtimeId } : {}),
    }),
  })
  return res.json()
}

export async function sendPrompt(
  socketPath: string,
  text: string,
  runtimeId?: string,
): Promise<{ ok: boolean }> {
  const res = await fetch('/api/prompt', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeader() },
    body: JSON.stringify({
      socket: socketPath,
      text,
      ...(runtimeId ? { runtime_id: runtimeId } : {}),
    }),
  })
  return res.json()
}

export async function fetchConfig(): Promise<AuthConfig> {
  const res = await fetch('/api/config')
  return res.json()
}

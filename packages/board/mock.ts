import type { Plugin } from 'vite'

// ── Mock data ──────────────────────────────────────────────────────────────

const SOCKETS = [
  {
    path: '/home/user/.avenor/sockets/supervisor-1.sock',
    type: 'stable',
    pid: 48291,
    status: {
      session_id: 'sess-abc123',
      run_id: 'run-7f3a',
      run_label: 'feat/auth-refactor',
      phase: 'running',
      phase_label: 'Running',
      last_event: 'tool.call',
      turn_state: 'in_progress',
      started_at: Date.now() - 120_000,
      updated_at: Date.now() - 2000,
      pending_permission: false,
    },
  },
  {
    path: '/home/user/.avenor/sockets/supervisor-2.sock',
    type: 'stable',
    pid: 51842,
    status: {
      session_id: 'sess-def456',
      run_id: 'run-9c2b',
      run_label: 'bug/timeout-fix',
      phase: 'waiting',
      phase_label: 'Waiting for permission',
      last_event: 'permission.request',
      started_at: Date.now() - 300_000,
      updated_at: Date.now() - 8000,
      pending_permission: true,
      permission: {
        tool_name: 'bash',
        input: { command: 'rm -rf /tmp/test-build' },
        options: [
          { id: 'allow_once', label: 'Allow once' },
          { id: 'deny', label: 'Deny' },
        ],
      },
    },
  },
  {
    path: '/home/user/.avenor/sockets/oneshot-3.sock',
    type: 'one-shot',
    pid: 49213,
    status: {
      session_id: 'sess-ghi789',
      run_id: 'run-e5d1',
      run_label: '',
      phase: 'completed',
      phase_label: 'Completed',
      last_event: 'session.end',
      started_at: Date.now() - 500_000,
      updated_at: Date.now() - 420_000,
      pending_permission: false,
    },
  },
]

const RUNTIMES: Record<string, Array<Record<string, unknown>>> = {
  '/home/user/.avenor/sockets/supervisor-1.sock': [
    {
      runtime_id: 'rt-alpha',
      label: 'mule-refactor',
      session_id: 'sess-abc123',
      status: 'running',
    },
    {
      runtime_id: 'rt-beta',
      label: 'horse-investigate',
      session_id: 'sess-abc123',
      status: 'idle',
    },
  ],
  '/home/user/.avenor/sockets/supervisor-2.sock': [
    {
      runtime_id: 'rt-gamma',
      label: 'butler-main',
      session_id: 'sess-def456',
      status: 'waiting',
    },
  ],
}

// Simulated SSE event types with realistic payloads
const EVENT_TYPES = [
  {
    event: 'agent.thought_chunk',
    data: {
      event: 'agent.thought_chunk',
      content: 'I need',
      model: 'glm-5.1',
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'agent.thought_chunk',
    data: {
      event: 'agent.thought_chunk',
      content: ' to refactor',
      model: 'glm-5.1',
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'agent.thought_chunk',
    data: {
      event: 'agent.thought_chunk',
      content: ' the auth module to use the new token rotation interface.',
      model: 'glm-5.1',
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'tool.call',
    data: {
      event: 'tool.call',
      tool_name: 'bash',
      tool_call_id: 'tc-001',
      input: { command: 'git diff HEAD~3 -- internal/auth/' },
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'tool.call',
    data: {
      event: 'tool.call',
      tool_name: 'read',
      tool_call_id: 'tc-002',
      input: { file_path: '/home/user/project/internal/auth/tokens.go' },
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'agent.status',
    data: {
      event: 'agent.status',
      phase: 'running',
      phase_label: 'Running',
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'agent.status',
    data: {
      event: 'agent.status',
      phase: 'thinking',
      phase_label: 'Thinking',
      run_id: 'rt-alpha',
    },
  },
  {
    event: 'permission.request',
    data: {
      event: 'permission.request',
      run_id: 'rt-alpha',
      tool_name: 'bash',
      input: { command: 'npm test -- --coverage' },
      options: [
        { id: 'allow_once', label: 'Allow once' },
        { id: 'deny', label: 'Deny' },
      ],
    },
  },
  {
    event: 'session.start',
    data: { event: 'session.start', run_id: 'rt-alpha' },
  },
  {
    event: 'session.end',
    data: { event: 'session.end', run_id: 'rt-alpha' },
  },
  {
    event: 'agent.thought_chunk',
    data: {
      event: 'agent.thought_chunk',
      content: 'The tests pass',
      model: 'glm-5.1',
      run_id: 'rt-beta',
    },
  },
  {
    event: 'agent.thought_chunk',
    data: {
      event: 'agent.thought_chunk',
      content: ' with 94% coverage.',
      model: 'glm-5.1',
      run_id: 'rt-beta',
    },
  },
]

// ── Vite plugin ─────────────────────────────────────────────────────────────

export function mockApiPlugin(): Plugin {
  return {
    name: 'mock-api',
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = new URL(req.url || '/', 'http://localhost')

        // JSON API endpoints
        if (req.method === 'GET' && url.pathname === '/api/sockets') {
          respondJson(res, SOCKETS)
          return
        }

        if (req.method === 'GET' && url.pathname === '/api/status') {
          const socket = url.searchParams.get('socket')
          const found = SOCKETS.find((s) => s.path === socket)
          respondJson(res, found ? found.status : { error: 'not found' }, found ? 200 : 404)
          return
        }

        if (req.method === 'GET' && url.pathname === '/api/list') {
          const socket = url.searchParams.get('socket')
          respondJson(res, RUNTIMES[socket] || [])
          return
        }

        if (req.method === 'GET' && url.pathname === '/api/config') {
          respondJson(res, { mutations_allowed: true, auth_required: false })
          return
        }

        // SSE endpoint — stream mock events
        if (req.method === 'GET' && url.pathname === '/api/events') {
          streamMockEvents(req, res)
          return
        }

        // Mutation endpoints — acknowledge and echo
        if (req.method === 'POST' && (url.pathname === '/api/cancel' || url.pathname === '/api/prompt')) {
          let body = ''
          req.on('data', (chunk: string) => { body += chunk })
          req.on('end', () => {
            respondJson(res, { ok: true, echoed: body })
          })
          return
        }

        next()
      })
    },
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function respondJson(res: any, data: unknown, status = 200) {
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  })
  res.end(JSON.stringify(data))
}

function streamMockEvents(req: any, res: any) {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  })

  // Send initial burst
  const initialEvents = [
    EVENT_TYPES.find((e) => e.event === 'session.start')!,
    EVENT_TYPES[0], // thought_chunk
    EVENT_TYPES[1], // tool.call (bash)
    EVENT_TYPES.find((e) => e.event === 'agent.status')!,
  ]

  let delay = 0
  for (const ev of initialEvents) {
    setTimeout(() => {
      if (res.writableEnded) return
      const data = JSON.stringify(ev.data)
      res.write(`event: event\ndata: ${data}\n\n`)
    }, delay)
    delay += 300
  }

  // Then stream random events every 2-4 seconds
  let idx = 0
  const interval = setInterval(() => {
    if (res.writableEnded) {
      clearInterval(interval)
      return
    }
    const ev = EVENT_TYPES[idx % EVENT_TYPES.length]
    const data = JSON.stringify(ev.data)
    res.write(`event: event\ndata: ${data}\n\n`)
    idx++
  }, 2000 + Math.random() * 2000)

  req.on('close', () => {
    clearInterval(interval)
  })
}
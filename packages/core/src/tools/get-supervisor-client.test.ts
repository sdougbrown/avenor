import { afterEach, beforeEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'

import { createOwnerGate, respond, type OwnerGateRequest } from '../../test/fixtures/owner-gate.js'
import { OwnerRecordMissingError } from '../owner-attach.js'
import { answerPermissionTool } from './answer-permission.js'
import { brokerReceiveTool } from './broker-receive.js'
import { getSupervisorClient } from './get-supervisor-client.js'
import { killTool } from './kill.js'
import { shutdownTool } from './shutdown.js'
import { statusTool } from './status.js'

describe('read-only control clients', () => {
  let home: string
  let previousHome: string | undefined
  let server: net.Server
  let socketPath: string

  beforeEach(async () => {
    previousHome = process.env.AVENOR_HOME
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-read-only-'))
    process.env.AVENOR_HOME = home
    const socketsDir = path.join(home, 'sockets')
    fs.mkdirSync(socketsDir, { recursive: true, mode: 0o700 })
    socketPath = path.join(socketsDir, `avenor-mcp-read-only-${process.pid}.sock`)

    server = net.createServer((socket) => {
      let buffer = ''
      socket.on('data', (chunk) => {
        buffer += chunk.toString()
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''
        for (const line of lines) {
          if (!line.trim()) continue
          const request = JSON.parse(line) as { id: number, method: string }
          const result = request.method === 'list'
            ? []
            : request.method === 'message_receive'
              ? { messages: [] }
              : undefined
          socket.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result }) + '\n')
        }
      })
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(socketPath, resolve)
    })
  })

  afterEach(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()))
    fs.rmSync(home, { recursive: true, force: true })
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
  })

  it('keeps every read-only tool on a credential-free dial', async () => {
    const { client } = await getSupervisorClient(socketPath)
    expect(await client.list()).toEqual([])
    client.close()

    expect(await brokerReceiveTool({ supervisorId: socketPath })).toEqual({ asks: [] })
    expect(await statusTool({ supervisorId: socketPath })).toMatchObject({ runs: [], count: 0 })
  })

  it('requires the published record for an owner attach', async () => {
    await expect(getSupervisorClient(socketPath, { owner: true })).rejects.toBeInstanceOf(
      OwnerRecordMissingError,
    )
  })
})

describe('owner-gated control tools', () => {
  let home: string
  let previousHome: string | undefined
  let server: net.Server
  let socketPath: string
  const received: string[] = []

  beforeEach(async () => {
    previousHome = process.env.AVENOR_HOME
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'avenor-gated-tools-'))
    process.env.AVENOR_HOME = home
    const socketsDir = path.join(home, 'sockets')
    fs.mkdirSync(socketsDir, { recursive: true, mode: 0o700 })
    socketPath = path.join(socketsDir, `avenor-mcp-gated-${process.pid}.sock`)
    received.length = 0

    const gate = createOwnerGate({ socketPath })
    const resultFor = (request: OwnerGateRequest): unknown => {
      switch (request.method) {
        case 'list':
          return [{ run_id: 'run-1', label: 'demo', runtime_id: 'rt-1', session_id: 'ses-1' }]
        case 'status':
          return { run_id: 'run-1', runtime_id: 'rt-1', status: 'running', session_id: 'ses-1' }
        default:
          return { ok: true }
      }
    }
    server = net.createServer((socket) => {
      let buffer = ''
      const session = gate.session(line => socket.write(line))
      socket.on('data', (chunk) => {
        buffer += chunk.toString()
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''
        for (const line of lines) {
          if (!line.trim()) continue
          const request = JSON.parse(line) as OwnerGateRequest
          if (session.handle(request)) continue
          if (gate.isGated(request.method) && !session.authenticated) {
            socket.write(gate.deny(request.id))
            continue
          }
          received.push(request.method)
          socket.write(respond(request.id, resultFor(request)))
        }
      })
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(socketPath, resolve)
    })
    gate.publish()
  })

  afterEach(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()))
    fs.rmSync(home, { recursive: true, force: true })
    if (previousHome === undefined) delete process.env.AVENOR_HOME
    else process.env.AVENOR_HOME = previousHome
  })

  // Each tool must reach its gated method over an authenticated connection; a
  // bare dial gets -32010 and never reaches the gate.
  const gatedTools: Array<{
    name: string
    method: string
    run: (supervisorId: string) => Promise<unknown>
  }> = [
    {
      name: 'kill',
      method: 'cancel',
      run: supervisorId => killTool({ runId: 'run-1', supervisorId }),
    },
    {
      name: 'answer-permission',
      method: 'answer_permission',
      run: supervisorId => answerPermissionTool({
        runId: 'run-1',
        optionId: 'allow_once',
        requestId: 'request-1',
        supervisorId,
      }),
    },
    {
      name: 'shutdown',
      method: 'shutdown',
      run: supervisorId => shutdownTool({ supervisorId }),
    },
  ]

  for (const testCase of gatedTools) {
    it(`attaches ${testCase.name} as owner before its gated method`, async () => {
      await testCase.run(socketPath)
      expect(received).toContain(testCase.method)
    })
  }
})

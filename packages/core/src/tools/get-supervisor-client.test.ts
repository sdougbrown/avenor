import { afterEach, beforeEach, describe, expect, it } from 'bun:test'
import * as fs from 'node:fs'
import * as net from 'node:net'
import * as os from 'node:os'
import * as path from 'node:path'

import { OwnerRecordMissingError } from '../owner-attach.js'
import { brokerReceiveTool } from './broker-receive.js'
import { getSupervisorClient } from './get-supervisor-client.js'
import { peersTool } from './peers.js'
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
            : request.method === 'broker_peers'
              ? { sessions: [{ run_id: 'peer' }] }
              : request.method === 'broker_receive'
                ? []
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

    expect(await peersTool({ supervisorId: socketPath })).toEqual({ peers: [{ run_id: 'peer' }] })
    expect(await brokerReceiveTool({ supervisorId: socketPath })).toEqual({ asks: [] })
    expect(await statusTool({ supervisorId: socketPath })).toMatchObject({ runs: [], count: 0 })
  })

  it('requires the published record for an owner attach', async () => {
    await expect(getSupervisorClient(socketPath, { owner: true })).rejects.toBeInstanceOf(
      OwnerRecordMissingError,
    )
  })
})

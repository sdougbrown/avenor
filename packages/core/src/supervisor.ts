import { spawn, execSync, type ChildProcess } from 'node:child_process'
import { accessSync, constants, unlinkSync } from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { dial, Client, type SpawnParams } from './client.js'

export interface RunInfo {
  label: string
  sentinelPath: string
  eventLogPath: string
  runtimeId?: string
  sessionId?: string
}

export interface SupervisorOptions {
  binaryPath?: string
  callTimeoutMs?: number
}

export function findAvenorBinary(): string {
  const envBin = process.env.AVENOR_BIN
  if (envBin) {
    try {
      accessSync(envBin, constants.X_OK)
      return envBin
    } catch {
      throw new Error(`AVENOR_BIN path not executable: ${envBin}`)
    }
  }

  try {
    const binPath = execSync('which avenor', { encoding: 'utf-8' }).trim()
    if (binPath) {
      accessSync(binPath, constants.X_OK)
      return binPath
    }
  } catch {
    // which failed or returned empty
  }

  const homeBin = path.join(os.homedir(), '.botfiles', 'bin', 'avenor')
  try {
    accessSync(homeBin, constants.X_OK)
    return homeBin
  } catch {
    // not found
  }

  throw new Error(
    "avenor binary not found. Set AVENOR_BIN=/path/to/avenor or ensure 'avenor' is on PATH.",
  )
}

export class Supervisor {
  private static instance: Supervisor | null = null
  private static cleanupRegistered = false

  private static registerCleanup(): void {
    if (Supervisor.cleanupRegistered) return
    Supervisor.cleanupRegistered = true
    const cleanup = () => {
      if (Supervisor.instance?.childProcess) {
        Supervisor.instance.childProcess.kill()
      }
    }
    process.on('exit', cleanup)
    process.on('SIGINT', () => { cleanup(); process.exit() })
    process.on('SIGTERM', () => { cleanup(); process.exit() })
  }

  private client: Client | null = null
  private childProcess: ChildProcess | null = null
  private socketPath: string
  private crashed = false
  private crashCode: number | null = null
  private runs = new Map<string, RunInfo>()
  private binaryPath: string
  private callTimeoutMs: number

  private constructor(
    binaryPath: string,
    socketPath: string,
    opts?: SupervisorOptions,
  ) {
    this.binaryPath = binaryPath
    this.socketPath = socketPath
    this.callTimeoutMs = opts?.callTimeoutMs ?? 30_000
  }

  static async get(opts?: SupervisorOptions): Promise<Supervisor> {
    if (Supervisor.instance && !Supervisor.instance.crashed) {
      return Supervisor.instance
    }

    Supervisor.registerCleanup()
    const binaryPath = opts?.binaryPath ?? findAvenorBinary()
    const socketPath = path.join(os.tmpdir(), `avenor-mcp-${process.pid}.sock`)

    let socketExists = false
    try {
      accessSync(socketPath, constants.R_OK | constants.W_OK)
      socketExists = true
    } catch {
      // socket does not exist
    }

    if (socketExists) {
      try {
        const probeClient = await dial(socketPath, { callTimeoutMs: 2000 })
        try {
          await probeClient.status()
          probeClient.close()
          const client = await dial(socketPath)
          const sup = new Supervisor(binaryPath, socketPath, opts)
          sup.client = client
          Supervisor.instance = sup
          return sup
        } catch {
          probeClient.close()
          try {
            unlinkSync(socketPath)
          } catch {
            // couldn't unlink
          }
        }
      } catch {
        try {
          unlinkSync(socketPath)
        } catch {
          // couldn't unlink
        }
      }
    }

    const sup = new Supervisor(binaryPath, socketPath, opts)
    await sup.startProcess()
    Supervisor.instance = sup
    return sup
  }

  private startProcess(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let resolved = false

      this.childProcess = spawn(
        this.binaryPath,
        [
          'stable',
          '--control-socket',
          this.socketPath,
          '--idle-timeout',
          '30m',
        ],
        { detached: false },
      )

      this.childProcess.on('exit', (code, signal) => {
        if (!resolved) {
          resolved = true
          reject(
            new Error(
              `avenor exited with code ${code}${signal ? ' signal ' + signal : ''} during startup`,
            ),
          )
          return
        }
        if (code !== 0 || signal) {
          this.crashed = true
          this.crashCode = code ?? -1
          if (this.client) {
            try {
              this.client.close()
            } catch {
              // ignore
            }
            this.client = null
          }
        }
      })

      this.childProcess.on('error', (err) => {
        if (!resolved) {
          resolved = true
          reject(new Error(`failed to start avenor: ${err.message}`))
        }
      })

      const timeout = setTimeout(() => {
        if (!resolved) {
          resolved = true
          reject(new Error('timed out waiting for avenor socket'))
        }
      }, 10_000)

      const retryDial = async () => {
        while (!resolved) {
          try {
            await new Promise<void>((r) => setTimeout(r, 100))
            this.client = await dial(this.socketPath, {
              callTimeoutMs: this.callTimeoutMs,
            })
            clearTimeout(timeout)
            resolved = true
            resolve()
            return
          } catch {
            // retry
          }
        }
      }
      retryDial()
    })
  }

  get supervisorId(): string {
    return this.socketPath
  }

  getClient(): Client {
    if (this.crashed) {
      throw new Error(
        `avenor supervisor crashed (exit code ${this.crashCode}). Call avenor_shutdown to clean up and retry.`,
      )
    }
    if (!this.client) {
      throw new Error('supervisor not started')
    }
    return this.client
  }

  async spawn(params: SpawnParams): Promise<RunInfo> {
    const client = this.getClient()
    const runId = crypto.randomUUID()
    const sentinelPath = path.join(os.tmpdir(), `avenor-run-${runId}.done`)
    const eventLogPath = path.join(os.tmpdir(), `avenor-run-${runId}.log`)

    const spawnParams: SpawnParams = {
      ...params,
      label: params.label ?? runId,
      sentinel_file: sentinelPath,
      on_event: eventLogPath,
    }

    const result = await client.spawn(spawnParams)

    const runInfo: RunInfo = {
      label: (spawnParams.label as string) ?? runId,
      sentinelPath: (result.sentinel_file as string) ?? sentinelPath,
      eventLogPath: (result.on_event as string) ?? eventLogPath,
      runtimeId: result.runtime_id as string | undefined,
      sessionId: result.session_id as string | undefined,
    }

    this.runs.set(runId, runInfo)
    return runInfo
  }

  async close(): Promise<void> {
    if (this.client) {
      try {
        await this.client.shutdown('graceful')
      } catch {
        // ignore shutdown errors
      }
    }

    if (this.childProcess) {
      this.childProcess.kill()
      this.childProcess = null
    }

    if (this.client) {
      try {
        this.client.close()
      } catch {
        // ignore close errors
      }
      this.client = null
    }

    try {
      unlinkSync(this.socketPath)
    } catch {
      // ignore
    }

    this.crashed = false
    this.crashCode = null
    this.runs.clear()
    Supervisor.instance = null
  }
}

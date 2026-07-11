import { spawn, execSync, type ChildProcess } from 'node:child_process'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { dial, Client, type SpawnParams } from './client.js'
import { ensureRunPaths, socketsRoot } from './paths.js'
import { installerBinaryPath } from './install-path.js'

export { installerBinaryPath } from './install-path.js'

export interface RunInfo {
  runId: string
  label: string
  sentinelPath: string
  eventLogPath: string
  runtimeId?: string
  sessionId?: string
  brokerUrl?: string
  parentToken?: string
}

export interface SupervisorOptions {
  binaryPath?: string
  callTimeoutMs?: number
}

export function findAvenorBinary(): string {
  const envBin = process.env.AVENOR_BIN
  if (envBin) {
    try {
      fs.accessSync(envBin, fs.constants.X_OK)
      return envBin
    } catch {
      throw new Error(`AVENOR_BIN path not executable: ${envBin}`)
    }
  }

  try {
    const binPath = execSync('which avenor', { encoding: 'utf-8', env: process.env }).trim()
    if (binPath) {
      fs.accessSync(binPath, fs.constants.X_OK)
      return binPath
    }
  } catch {
    // which failed or returned empty
  }

  try {
    const installPath = installerBinaryPath()
    fs.accessSync(installPath, fs.constants.X_OK)
    return installPath
  } catch {
    // installer-managed binary not found
  }

  const homeBin = path.join(os.homedir(), '.botfiles', 'bin', 'avenor')
  try {
    fs.accessSync(homeBin, fs.constants.X_OK)
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

    const socketDir = socketsRoot()
    fs.mkdirSync(socketDir, { recursive: true, mode: 0o700 })
    const socketPath = path.join(socketDir, `avenor-mcp-${process.pid}.sock`)

    let socketExists = false
    try {
      await fs.promises.access(socketPath, fs.constants.R_OK | fs.constants.W_OK)
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
            await fs.promises.unlink(socketPath)
          } catch {
            // couldn't unlink
          }
        }
      } catch {
        try {
          await fs.promises.unlink(socketPath)
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

  static isCurrentInstance(supervisorId: string): boolean {
    return Supervisor.instance !== null && Supervisor.instance.supervisorId === supervisorId
  }

  static currentInstance(): Supervisor | null {
    return Supervisor.instance
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

  async spawn(params: SpawnParams, runId = crypto.randomUUID()): Promise<RunInfo> {
    const client = this.getClient()
    const { sentinelPath, eventLogPath } = ensureRunPaths(runId)

    const spawnParams: SpawnParams = {
      ...params,
      label: params.label ?? runId,
      sentinel_file: sentinelPath,
      on_event: eventLogPath,
    }

    const result = await client.spawn(spawnParams)

    const runInfo: RunInfo = {
      runId,
      label: (spawnParams.label as string) ?? runId,
      sentinelPath: (result.sentinel_file as string) ?? sentinelPath,
      eventLogPath: (result.on_event as string) ?? eventLogPath,
      runtimeId: result.runtime_id as string | undefined,
      sessionId: result.session_id as string | undefined,
      brokerUrl: result.broker_url as string | undefined,
      parentToken: result.parent_token as string | undefined,
    }

    this.runs.set(runInfo.label, runInfo)
    this.runs.set(runId, runInfo)
    return runInfo
  }

  async close(options: { skipShutdown?: boolean } = {}): Promise<void> {
    if (this.client && !options.skipShutdown) {
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
      await fs.promises.unlink(this.socketPath)
    } catch {
      // ignore
    }

    this.crashed = false
    this.crashCode = null
    this.runs.clear()
    Supervisor.instance = null
  }
}

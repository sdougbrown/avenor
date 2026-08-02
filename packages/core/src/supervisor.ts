import { spawn, execSync, type ChildProcess } from 'node:child_process'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { dial, Client, type SpawnParams, type SpawnResult, type ThinkingLevel } from './client.js'
import { validateSpawnSelection } from './spawn-selection.js'
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
  agent?: string
  agentProfile?: string
  backend?: string
  model?: string
  effectiveAgent?: string
  effectiveModel?: string
  effectiveBackend?: string
  rosterFile?: string
  rosterEntry?: string
  thinking?: ThinkingLevel
  dir?: string
  brokerUrl?: string
  parentToken?: string
  autoApprove?: boolean
}

export function retainLiveIdentity(runInfo: RunInfo, liveStatus: Record<string, unknown>): void {
  const value = (candidate: unknown): string | undefined =>
    typeof candidate === 'string' && candidate.length > 0 ? candidate : undefined
  const liveIdentity = (effectiveKey: string, directKey: string): { present: boolean, value?: string } => {
    if (typeof liveStatus[effectiveKey] === 'string') {
      return { present: true, value: value(liveStatus[effectiveKey]) }
    }
    if (typeof liveStatus[directKey] === 'string') {
      return { present: true, value: value(liveStatus[directKey]) }
    }
    return { present: false }
  }
  const effectiveAgent = liveIdentity('effective_agent', 'agent')
  const effectiveModel = liveIdentity('effective_model', 'model')
  const effectiveBackend = liveIdentity('effective_backend', 'backend')
  const sessionId = value(liveStatus.session_id)
  const rosterFilePresent = typeof liveStatus.roster_file === 'string'
  const rosterFile = value(liveStatus.roster_file)
  const rosterEntryPresent = typeof liveStatus.roster_entry === 'string'
  const rosterEntry = value(liveStatus.roster_entry)
  const agentProfilePresent = typeof liveStatus.agent_profile === 'string'
  const agentProfile = value(liveStatus.agent_profile)

  if (effectiveAgent.present) runInfo.agent = runInfo.effectiveAgent = effectiveAgent.value
  if (effectiveModel.present) runInfo.model = runInfo.effectiveModel = effectiveModel.value
  if (effectiveBackend.present) runInfo.backend = runInfo.effectiveBackend = effectiveBackend.value
  if (sessionId) runInfo.sessionId = sessionId
  if (rosterFilePresent) runInfo.rosterFile = rosterFile
  if (rosterEntryPresent) runInfo.rosterEntry = rosterEntry
  if (agentProfilePresent) runInfo.agentProfile = agentProfile
}

export interface SupervisorOptions {
  binaryPath?: string
  callTimeoutMs?: number
}

/** Metadata retained locally when a follow-up uses a resolved direct identity. */
export interface SpawnMetadata {
  rosterFile?: string
  rosterEntry?: string
  effectiveAgent?: string
  effectiveModel?: string
  effectiveBackend?: string
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
  private static starting: Promise<Supervisor> | null = null
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
  // Public run IDs are canonical; labels are lookup-only aliases.
  private runs = new Map<string, RunInfo>()
  private aliases = new Map<string, RunInfo>()
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

  static get(opts?: SupervisorOptions): Promise<Supervisor> {
    const current = Supervisor.instance
    if (current && !current.crashed) {
      if (current.client && !current.client.isClosed()) {
        return Promise.resolve(current)
      }

      // The peer may close the control socket before the child-process exit
      // event runs. Invalidate the stale singleton immediately so the next
      // caller starts a replacement instead of retrying a dead socket.
      current.crashed = true
      current.crashCode = 0
      current.client?.close()
      current.client = null
    }
    if (Supervisor.starting) return Supervisor.starting

    const starting = Supervisor.start(opts)
    Supervisor.starting = starting
    const clearStarting = () => {
      if (Supervisor.starting === starting) Supervisor.starting = null
    }
    void starting.then(clearStarting, clearStarting)
    return starting
  }

  private static async start(opts?: SupervisorOptions): Promise<Supervisor> {
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
        // A clean supervisor exit still invalidates the cached client (for
        // example, after an explicit shutdown or idle timeout). Treat every
        // post-start exit as unavailable so the next get() can reconnect.
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
      const state = this.crashCode === 0 ? 'exited' : 'crashed'
      throw new Error(
        `avenor supervisor ${state} (exit code ${this.crashCode}). Call avenor_shutdown to clean up and retry.`,
      )
    }
    if (!this.client) {
      throw new Error('supervisor not started')
    }
    if (this.client.isClosed()) {
      this.crashed = true
      this.crashCode = 0
      this.client = null
      throw new Error('avenor supervisor connection closed')
    }
    return this.client
  }

  async spawn(
    params: SpawnParams,
    runId = crypto.randomUUID(),
    metadata?: SpawnMetadata,
  ): Promise<RunInfo> {
    const workflowMode = typeof params.loop_file === 'string' || typeof params.team_file === 'string'
    if (!workflowMode) {
      validateSpawnSelection({
        agent: params.agent,
        model: params.model,
        backend: params.backend,
        roster_file: params.roster_file,
        roster_entry: params.roster_entry,
      })
    }

    const client = this.getClient()
    const { sentinelPath, eventLogPath } = ensureRunPaths(runId)

    const spawnParams: SpawnParams = {
      ...params,
      label: params.label ?? runId,
      sentinel_file: sentinelPath,
      on_event: eventLogPath,
    }
    if (spawnParams.roster_entry) {
      // Empty optional direct values are equivalent to omission in the shared
      // selector contract; do not forward them as roster overrides.
      for (const field of ['agent', 'model', 'backend'] as const) {
        if (spawnParams[field] === '') delete spawnParams[field]
      }
    }

    const result = await client.spawn(spawnParams)
    let identityResult: SpawnResult = result

    // Older supervisors may return only runtime/session identifiers from spawn.
    // A status lookup fills in the resolved roster identity without rereading the
    // roster file. Failure is deliberately non-fatal: direct compatibility still
    // has the caller-supplied identity as a fallback.
    if (spawnParams.roster_entry && result.runtime_id) {
      const hasBackend = [result.effective_backend, result.backend]
        .some(value => typeof value === 'string' && value.length > 0)
      const hasAgentOrModel = [
        result.effective_agent,
        result.agent,
        result.effective_model,
        result.model,
      ].some(value => typeof value === 'string' && value.length > 0)
      const hasIdentity = hasBackend && hasAgentOrModel
      if (!hasIdentity) {
        try {
          identityResult = {
            ...result,
            ...(await client.status(result.runtime_id)),
          }
        } catch {
          // The spawn already succeeded; retain selector metadata and fallbacks.
        }
      }
    }

    const value = (candidate: unknown): string | undefined =>
      typeof candidate === 'string' && candidate.length > 0 ? candidate : undefined
    const effectiveAgent = metadata?.effectiveAgent ??
      value(identityResult.effective_agent) ?? value(identityResult.agent) ?? value(spawnParams.agent)
    const effectiveModel = metadata?.effectiveModel ??
      value(identityResult.effective_model) ?? value(identityResult.model) ?? value(spawnParams.model)
    const effectiveBackend = metadata?.effectiveBackend ??
      value(identityResult.effective_backend) ?? value(identityResult.backend) ?? value(spawnParams.backend)
    const rosterFile = metadata?.rosterFile ??
      value(identityResult.roster_file) ?? value(spawnParams.roster_file)
    const rosterEntry = metadata?.rosterEntry ??
      value(identityResult.roster_entry) ?? value(spawnParams.roster_entry)

    const runInfo: RunInfo = {
      runId,
      label: (spawnParams.label as string) ?? runId,
      sentinelPath: (result.sentinel_file as string) ?? sentinelPath,
      eventLogPath: (result.on_event as string) ?? eventLogPath,
      runtimeId: value(identityResult.runtime_id),
      sessionId: value(identityResult.session_id),
      agent: effectiveAgent,
      agentProfile: value(identityResult.agent_profile) ?? (spawnParams.agent_profile as string | undefined),
      backend: effectiveBackend,
      model: effectiveModel,
      effectiveAgent,
      effectiveModel,
      effectiveBackend,
      rosterFile,
      rosterEntry,
      thinking: spawnParams.thinking,
      dir: spawnParams.dir as string | undefined,
      brokerUrl: result.broker_url as string | undefined,
      parentToken: result.parent_token as string | undefined,
      autoApprove: spawnParams.auto_approve,
    }

    this.runs.set(runId, runInfo)
    this.aliases.set(runInfo.label, runInfo)
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
    this.aliases.clear()
    Supervisor.instance = null
  }
}

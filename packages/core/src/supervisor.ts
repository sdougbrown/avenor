import { spawn, execSync, type ChildProcess } from 'node:child_process'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import { dial, Client, OwnerAuthError, type SpawnParams, type SpawnResult, type ThinkingLevel } from './client.js'
import {
  attachOwner,
  ForeignPeerError,
  loadOwnerRecord,
  OwnerRecordMissingError,
  OwnerSocketAbsentError,
  probeControlSocket,
  StaleOwnerRecordError,
  validateOwnerRecordSocket,
} from './owner-attach.js'
import { validateSpawnSelection } from './spawn-selection.js'
import { ensureRunPaths, socketsRoot, validateSupervisorSocketPath } from './paths.js'
import { installerBinaryPath } from './install-path.js'

export { installerBinaryPath } from './install-path.js'

const STARTUP_TIMEOUT_MS = 10_000
const STARTUP_PROBE_CALL_TIMEOUT_MS = 1_000
const CHILD_REAP_GRACE_MS = 5_000

type ExistingSupervisorState = 'live' | 'dead' | 'auth-rejected' | 'uncertain'

interface ExistingSupervisor {
  client?: Client
  state: ExistingSupervisorState
  error?: Error
}

// Only a peer that refused, did not support or timed out the capability, or a
// path this euid cannot bind, is an authentication outcome.
function isOwnerRejection(err: unknown): boolean {
  return err instanceof OwnerAuthError ||
    err instanceof ForeignPeerError ||
    err instanceof OwnerRecordMissingError ||
    err instanceof StaleOwnerRecordError
}

// Classification of an attach that followed a live probe. An authentication
// outcome is terminal. Everything else, including a socket that is absent or
// refusing by now, is uncertainty and authorizes no replacement: a socket can
// vanish while its owner is still shutting down, so only a fresh probe that
// finds no listener is proof of a dead leftover.
function classifyAttachError(err: unknown): ExistingSupervisorState {
  return isOwnerRejection(err) ? 'auth-rejected' : 'uncertain'
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/** Wait until the Go server has removed its socket, bounded by the timeout. */
async function waitForSocketRemoval(socketPath: string, timeoutMs = CHILD_REAP_GRACE_MS): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      fs.lstatSync(socketPath)
    } catch {
      return
    }
    await sleep(50)
  }
}

/** Wait for the child to exit on its own, bounded by the timeout. */
async function waitForChildExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (child.exitCode !== null || child.signalCode !== null) return true
  return new Promise<boolean>((resolve) => {
    const onExit = () => {
      clearTimeout(timer)
      resolve(true)
    }
    const timer = setTimeout(() => {
      child.removeListener('exit', onExit)
      resolve(false)
    }, timeoutMs)
    child.once('exit', onExit)
  })
}

/** Terminate the captured child and wait for it to exit. */
async function reapChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return
  const exited = new Promise<void>(resolve => child.once('exit', () => resolve()))
  try {
    child.kill('SIGTERM')
  } catch {
    return
  }
  const escalate = setTimeout(() => {
    try {
      child.kill('SIGKILL')
    } catch {
      // already gone
    }
  }, CHILD_REAP_GRACE_MS)
  await exited
  clearTimeout(escalate)
}

/**
 * Classify an existing control socket path without presenting a credential.
 * A bound unix listener never refuses, so ECONNREFUSED (or an absent path)
 * proves no listener holds it; anything else is uncertain.
 */
async function attachExistingSupervisor(socketPath: string): Promise<ExistingSupervisor> {
  const probe = await probeControlSocket(socketPath)
  if (probe !== 'live') {
    return {
      state: probe,
      error: probe === 'uncertain'
        ? new Error(`control socket probe for ${socketPath} was inconclusive`)
        : undefined,
    }
  }
  try {
    return { client: await attachOwner(socketPath), state: 'live' }
  } catch (err) {
    return { state: classifyAttachError(err), error: err instanceof Error ? err : new Error(String(err)) }
  }
}

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
    const socketPath = path.join(fs.realpathSync(socketDir), `avenor-mcp-${process.pid}.sock`)

    let socketExists = false
    try {
      await fs.promises.access(socketPath, fs.constants.R_OK | fs.constants.W_OK)
      socketExists = true
    } catch {
      // socket does not exist
    }

    if (socketExists) {
      const existing = await attachExistingSupervisor(socketPath)
      if (existing.client) {
        const sup = new Supervisor(binaryPath, socketPath, opts)
        sup.client = existing.client
        // A crashed instance may still hold the child that owns this socket;
        // carrying the handle keeps its cleanup reachable from the new one.
        const previous = Supervisor.instance
        if (previous && previous.socketPath === socketPath && previous.childProcess) {
          sup.childProcess = previous.childProcess
          previous.childProcess = null
        }
        Supervisor.instance = sup
        return sup
      }
      if (existing.state !== 'dead') {
        // A live path this process cannot authenticate to, or a probe that
        // proves nothing, is terminal: never unlink it, never start over it.
        throw existing.error ?? new Error(`control socket ${socketPath} is not attachable`)
      }
      // The path is a dead-owner leftover. The new server is the only authority
      // that may remove it, so the launcher leaves every artifact in place.
    }

    const sup = new Supervisor(binaryPath, socketPath, opts)
    await sup.startProcess()
    Supervisor.instance = sup
    return sup
  }

  private startProcess(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      let settled = false
      let ignoreChildExit = false
      const attemptToken = crypto.randomBytes(32).toString('hex')
      const deadline = Date.now() + STARTUP_TIMEOUT_MS

      const child = spawn(
        this.binaryPath,
        [
          'stable',
          '--control-socket',
          this.socketPath,
          '--idle-timeout',
          '30m',
        ],
        {
          detached: false,
          env: { ...process.env, AVENOR_CONTROL_IDENTITY_TOKEN: attemptToken },
        },
      )
      this.childProcess = child

      const settleFailure = (err: Error): void => {
        if (settled) return
        settled = true
        ignoreChildExit = true
        void reapChild(child).finally(() => {
          if (this.childProcess === child) this.childProcess = null
          reject(err)
        })
      }

      child.on('exit', (code, signal) => {
        if (ignoreChildExit) return
        if (!settled) {
          // A competing starter may have won the bind, so re-check before
          // declaring this attempt failed: probe the path through its published
          // record, attach the winner when it is live, and never unlink it.
          const exitError = new Error(
            `avenor exited with code ${code}${signal ? ' signal ' + signal : ''} during startup`,
          )
          settled = true
          ignoreChildExit = true
          void (async () => {
            if (this.childProcess === child) this.childProcess = null
            try {
              this.client = await attachOwner(this.socketPath)
              resolve()
            } catch {
              reject(exitError)
            }
          })()
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

      child.on('error', (err) => {
        settleFailure(new Error(`failed to start avenor: ${err.message}`))
      })

      const attachOwned = async (): Promise<void> => {
        while (!settled) {
          if (Date.now() >= deadline) {
            settleFailure(new Error('timed out waiting for avenor socket'))
            return
          }
          if (child.exitCode !== null || child.signalCode !== null) return

          let client: Client
          try {
            // The connect is bounded by what is left of the attempt, so a hung
            // dial cannot outlive the startup deadline.
            client = await dial(this.socketPath, {
              callTimeoutMs: this.callTimeoutMs,
              connectTimeoutMs: Math.max(1, deadline - Date.now()),
            })
          } catch {
            await sleep(100)
            continue
          }
          if (settled) {
            client.close()
            return
          }

          let token: string
          try {
            token = await client.identity(STARTUP_PROBE_CALL_TIMEOUT_MS)
          } catch {
            client.close()
            if (settled) return
            await sleep(100)
            continue
          }
          if (settled) {
            client.close()
            return
          }

          if (token !== attemptToken) {
            client.close()
            // A competing starter holds the path. Retire only the child this
            // attempt spawned, then attach the winner through its record.
            settled = true
            ignoreChildExit = true
            await reapChild(child)
            try {
              this.client = await attachOwner(this.socketPath)
              resolve()
            } catch (err) {
              reject(new Error(`supervisor exited during startup: ${errorMessage(err)}`))
            }
            return
          }

          try {
            const record = loadOwnerRecord(this.socketPath)
            validateOwnerRecordSocket(this.socketPath, record)
            if (record.owner_pid !== child.pid) {
              throw new StaleOwnerRecordError(
                this.socketPath,
                `record names pid ${record.owner_pid}, not this child ${child.pid}`,
              )
            }
            // This connection answered with this attempt's token, and the
            // record it just published names this child, so the capability is
            // this child's. Presenting it on this connection cannot hand a
            // competing winner's secret to this launcher.
            await client.authenticateOwner(
              { id: record.capability_id, secret: record.secret },
              STARTUP_PROBE_CALL_TIMEOUT_MS * 2,
            )
            this.client = client
            settled = true
            resolve()
            return
          } catch (err) {
            client.close()
            if (settled) return
            if (err instanceof OwnerRecordMissingError || err instanceof StaleOwnerRecordError) {
              // The predecessor's record is still the live one, or publication
              // has not happened yet. Retry until the attempt deadline.
              await sleep(100)
              continue
            }
            if (err instanceof OwnerSocketAbsentError) {
              // The path has no listener to talk to yet; the child may still
              // publish its own socket before the deadline.
              await sleep(100)
              continue
            }
            settleFailure(err instanceof Error ? err : new Error(String(err)))
            return
          }
        }
      }
      void attachOwned()
    })
  }

  get supervisorId(): string {
    return this.socketPath
  }

  /**
   * Look up a run by its public run ID first, then by its label alias.
   * Public run IDs are canonical; labels are lookup-only aliases.
   */
  getRunByReference(reference: string): RunInfo | undefined {
    return this.runs.get(reference) ?? this.aliases.get(reference)
  }

  /**
   * Look up a run by its public run ID only, never by a label alias. The
   * messaging router resolves public IDs and labels on separate planes, so a
   * label that happens to equal another run's old ID must not match here.
   */
  getRunById(runId: string): RunInfo | undefined {
    return this.runs.get(runId)
  }

  /**
   * Register a run this supervisor resumed, so its ID and label stay resolvable.
   * Last write wins: an existing entry with the same run ID, or a label held by
   * another run, is replaced. Duplicate rejection is the resume provider's job.
   */
  registerResumed(run: RunInfo): void {
    this.register(run)
  }

  /** The one place a run's ID and label are registered, for spawn and for resume. */
  private register(run: RunInfo): void {
    this.runs.set(run.runId, run)
    this.aliases.set(run.label, run)
  }

  /** Match a stored run by its broker runtime ID. */
  getRunByRuntimeId(runtimeId: string): RunInfo | undefined {
    if (!runtimeId) return undefined
    for (const runInfo of this.runs.values()) {
      if (runInfo.runtimeId === runtimeId) return runInfo
    }
    return undefined
  }

  /**
   * The live singleton when it owns supervisorId, null otherwise. Single
   * source of truth for the ownership check, so callers cannot observe
   * isCurrentInstance true while currentInstance is null.
   */
  static getInstance(supervisorId: string): Supervisor | null {
    if (Supervisor.instance === null) return null
    if (Supervisor.instance.supervisorId === supervisorId) return Supervisor.instance
    try {
      return Supervisor.instance.supervisorId === validateSupervisorSocketPath(supervisorId)
        ? Supervisor.instance
        : null
    } catch {
      return null
    }
  }

  static isCurrentInstance(supervisorId: string): boolean {
    return Supervisor.getInstance(supervisorId) !== null
  }

  static currentInstance(): Supervisor | null {
    return Supervisor.instance
  }

  /** Iterate stored runs without exposing the internal map. */
  forEachRun(callback: (info: RunInfo) => void): void {
    for (const runInfo of this.runs.values()) {
      callback(runInfo)
    }
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
    runId: string = crypto.randomUUID(),
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

    this.register(runInfo)
    return runInfo
  }

  async close(options: { skipShutdown?: boolean } = {}): Promise<void> {
    let shutdownSent = false
    if (this.client && !options.skipShutdown) {
      // Track the request rather than its answer: a shutdown that errors or
      // times out still gets the grace period below.
      shutdownSent = true
      try {
        await this.client.shutdown('graceful')
      } catch {
        // ignore shutdown errors
      }
    }

    if (this.childProcess) {
      const child = this.childProcess
      this.childProcess = null
      // The Go server traps only interrupts, so signalling it before it has
      // finished shutting down skips the cleanup that removes its socket and
      // owner record. Give it the grace period to exit on its own first.
      if (!shutdownSent || !(await waitForChildExit(child, CHILD_REAP_GRACE_MS))) {
        await reapChild(child)
      }
      // The Go server removes its own socket and owner record while it shuts
      // down; the launcher never unlinks either path.
      await waitForSocketRemoval(this.socketPath)
    } else if (shutdownSent) {
      // An attached supervisor is not this launcher's child, so the only
      // completion signal is the server's own cleanup.
      await waitForSocketRemoval(this.socketPath)
    }

    if (this.client) {
      try {
        this.client.close()
      } catch {
        // ignore close errors
      }
      this.client = null
    }

    this.crashed = false
    this.crashCode = null
    this.runs.clear()
    this.aliases.clear()
    Supervisor.instance = null
  }
}

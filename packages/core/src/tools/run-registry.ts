import * as fs from 'node:fs'
import * as path from 'node:path'
import * as crypto from 'node:crypto'
import type { RunInfo } from '../supervisor.js'
import { runsRoot } from '../paths.js'

export interface ExternalRunInfo extends RunInfo {
  supervisorId: string
}

type PersistedExternalRun = ExternalRunInfo & { schemaVersion: 1 }
type SupervisorRuns = {
  byRunId: Map<string, ExternalRunInfo>
  byAlias: Map<string, ExternalRunInfo>
}

const externalRuns = new Map<string, SupervisorRuns>()

function runsFor(supervisorId: string): SupervisorRuns {
  let runs = externalRuns.get(supervisorId)
  if (!runs) {
    runs = { byRunId: new Map(), byAlias: new Map() }
    externalRuns.set(supervisorId, runs)
  }
  return runs
}

function indexRun(runInfo: ExternalRunInfo): void {
  const runs = runsFor(runInfo.supervisorId)
  runs.byRunId.set(runInfo.runId, runInfo)
  runs.byAlias.set(runInfo.label, runInfo)
  if (runInfo.runtimeId) runs.byAlias.set(runInfo.runtimeId, runInfo)
}

function isSafeRunReference(reference: string): boolean {
  return /^[a-zA-Z0-9_-]+$/.test(reference)
}

export function externalRunMetadataPath(runId: string): string {
  if (!isSafeRunReference(runId)) {
    throw new Error(`unsafe external run reference: ${runId}`)
  }
  return path.join(runsRoot(), runId, 'run.json')
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function readPersistedRun(
  supervisorId: string,
  runId: string,
): ExternalRunInfo | undefined {
  if (!isSafeRunReference(runId)) return undefined

  try {
    const parsed = JSON.parse(
      fs.readFileSync(externalRunMetadataPath(runId), 'utf-8'),
    ) as Record<string, unknown>
    if (
      parsed.schemaVersion !== 1
      || parsed.runId !== runId
      || parsed.supervisorId !== supervisorId
      || typeof parsed.label !== 'string'
    ) {
      return undefined
    }

    const runDir = path.join(runsRoot(), runId)
    const runInfo: ExternalRunInfo = {
      runId,
      label: parsed.label,
      supervisorId,
      sentinelPath: path.join(runDir, 'sentinel.done'),
      eventLogPath: path.join(runDir, 'events.log'),
      runtimeId: optionalString(parsed.runtimeId),
      sessionId: optionalString(parsed.sessionId),
      agent: optionalString(parsed.agent),
      agentProfile: optionalString(parsed.agentProfile),
      backend: optionalString(parsed.backend),
      model: optionalString(parsed.model),
      effectiveAgent: optionalString(parsed.effectiveAgent),
      effectiveModel: optionalString(parsed.effectiveModel),
      effectiveBackend: optionalString(parsed.effectiveBackend),
      rosterFile: optionalString(parsed.rosterFile),
      rosterEntry: optionalString(parsed.rosterEntry),
      thinking: parsed.thinking as RunInfo['thinking'],
      dir: optionalString(parsed.dir),
      brokerUrl: optionalString(parsed.brokerUrl),
      parentToken: optionalString(parsed.parentToken),
      autoApprove: typeof parsed.autoApprove === 'boolean' ? parsed.autoApprove : undefined,
    }
    indexRun(runInfo)
    return runInfo
  } catch {
    return undefined
  }
}

/** Persist the public-to-runtime identity for runs spawned through an external supervisor. */
export function registerExternalRun(runInfo: ExternalRunInfo): void {
  const persisted: PersistedExternalRun = {
    schemaVersion: 1,
    ...runInfo,
    // Broker tokens are credentials for live channel traffic, not run identity.
    parentToken: undefined,
  }
  const metadataPath = externalRunMetadataPath(runInfo.runId)
  const temporaryPath = `${metadataPath}.${process.pid}.${crypto.randomUUID()}.tmp`
  fs.writeFileSync(temporaryPath, `${JSON.stringify(persisted, null, 2)}\n`, {
    mode: 0o600,
    flag: 'wx',
  })
  try {
    fs.renameSync(temporaryPath, metadataPath)
  } catch (error) {
    try { fs.unlinkSync(temporaryPath) } catch {}
    throw error
  }
  indexRun(runInfo)
}

export function findExternalRun(
  supervisorId: string,
  reference: string,
): ExternalRunInfo | undefined {
  const exact = externalRuns.get(supervisorId)?.byRunId.get(reference)
  if (exact) return exact
  const persisted = readPersistedRun(supervisorId, reference)
  if (persisted) return persisted
  listExternalRuns(supervisorId)
  return externalRuns.get(supervisorId)?.byAlias.get(reference)
}

export function listExternalRuns(supervisorId: string): ExternalRunInfo[] {
  try {
    for (const entry of fs.readdirSync(runsRoot(), { withFileTypes: true })) {
      if (entry.isDirectory()) readPersistedRun(supervisorId, entry.name)
    }
  } catch {
    // The runs root may not exist yet.
  }
  return [...(externalRuns.get(supervisorId)?.byRunId.values() ?? [])]
}

export function forgetExternalRuns(supervisorId: string): void {
  externalRuns.delete(supervisorId)
}

export function clearExternalRuns(supervisorId: string): ExternalRunInfo[] {
  const runs = listExternalRuns(supervisorId)
  forgetExternalRuns(supervisorId)
  return runs
}

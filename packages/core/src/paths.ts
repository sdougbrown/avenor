import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

export function avenorHome(): string {
  return process.env.AVENOR_HOME || path.join(os.homedir(), '.avenor')
}

export function socketsRoot(): string {
  return path.join(avenorHome(), 'sockets')
}

export function runsRoot(): string {
  return path.join(avenorHome(), 'runs')
}

export function ensureRunPaths(runId: string): {
  runDir: string
  sentinelPath: string
  eventLogPath: string
} {
  const runDir = path.join(runsRoot(), runId)
  fs.mkdirSync(runDir, { recursive: true, mode: 0o700 })
  return {
    runDir,
    sentinelPath: path.join(runDir, 'sentinel.done'),
    eventLogPath: path.join(runDir, 'events.log'),
  }
}

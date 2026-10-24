import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'

export function avenorHome(): string {
  return process.env.AVENOR_HOME || path.join(os.homedir(), '.avenor')
}

export function socketsRoot(): string {
  return path.join(avenorHome(), 'sockets')
}

export function validateSupervisorSocketPath(supervisorId: string): string {
  const root = fs.realpathSync(socketsRoot())
  const parent = fs.realpathSync(path.dirname(supervisorId))
  const basename = path.basename(supervisorId)

  // Do not realpath the socket itself. On macOS, Bun's realpathSync can fail
  // with EOPNOTSUPP while lstat-ing a live Unix-domain socket. Canonicalizing
  // the containing directory still prevents traversal and symlink escapes;
  // supervisor sockets are created directly in the private sockets root.
  if (
    parent !== root ||
    !/^avenor-mcp-[^/\\]+\.sock$/.test(basename)
  ) {
    throw new Error(`invalid supervisorId: ${supervisorId}`)
  }
  return path.join(parent, basename)
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

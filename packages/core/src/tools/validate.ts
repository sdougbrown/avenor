import * as fs from 'node:fs'
import * as path from 'node:path'
import { socketsRoot } from '../paths.js'

export function validateRunId(id: string): void {
  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    throw new Error(`invalid run_id: ${id}`)
  }
}

export function validateTimeout(value: string): number {
  const trimmed = value.trim()
  const match = /^(\d+)([smh]?)$/.exec(trimmed)
  if (!match) {
    throw new Error(`invalid timeout: ${value}`)
  }
  const amount = Number(match[1])
  const unit = match[2]
  if (!Number.isFinite(amount) || amount <= 0) {
    throw new Error(`invalid timeout: ${value}`)
  }
  if (unit === 'm') return amount * 60
  if (unit === 'h') return amount * 3600
  return amount
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

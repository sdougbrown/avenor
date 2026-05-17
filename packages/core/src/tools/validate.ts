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
  const canonical = fs.realpathSync(supervisorId)
  const root = fs.realpathSync(socketsRoot())
  const relative = path.relative(root, canonical)
  if (
    relative.startsWith('..') ||
    path.isAbsolute(relative) ||
    !/^avenor-mcp-[^/\\]+\.sock$/.test(path.basename(canonical))
  ) {
    throw new Error(`invalid supervisorId: ${supervisorId}`)
  }
  return canonical
}

import { execFile } from 'node:child_process'
import * as nodePath from 'node:path'
import {
  MAX_SESSION_NAME_RUNES,
  RESERVED_SESSION_REFERENCES,
  boundedRunes,
  stripDisallowedSessionNameRunes,
} from '@dougbots/avenor-core'

/**
 * The work-derived default display label for a bound conversation: the base
 * name of the git top-level of the session's directory, plus "@<branch>" when
 * the checked-out branch is not one of the conventional default names. It
 * mirrors the supervisor's own session-label derivation so a conversation that
 * never carries a native title still appears under the work it serves.
 */

// The repository's real default branch would mean resolving origin/HEAD — a
// config or network read on every derivation — so the derivation treats the
// two conventional names as the default ones instead and carries any other
// branch in the label.
const DEFAULT_BRANCH_NAMES = new Set(['main', 'master'])

const GIT_TIMEOUT_MS = 5_000

/** Read one git answer for dir, or an empty string when git fails or hangs. */
export type GitReader = (dir: string, args: string[]) => Promise<string>

function defaultGitReader(dir: string, args: string[]): Promise<string> {
  return new Promise(resolve => {
    execFile('git', ['-C', dir, ...args], { timeout: GIT_TIMEOUT_MS }, (error, stdout) => {
      resolve(error ? '' : stdout.trim())
    })
  })
}

/**
 * Turn a derived label into one the session name rule accepts: runes no
 * identifier may hold are dropped, path separators and whitespace runs become
 * a single separator, and the result is truncated to the name limit without
 * splitting a rune, then trimmed of separators.
 */
export function sanitizeSessionLabel(label: string): string {
  const stripped = stripDisallowedSessionNameRunes(label)
  const separated = stripped.replace(/[/\\\s]+/gu, '-')
  const collapsed = separated.replace(/-{2,}/g, '-')
  const truncated = boundedRunes(collapsed, MAX_SESSION_NAME_RUNES)
  return truncated.replace(/^-+|-+$/g, '')
}

export function labelUsable(label: string): boolean {
  // The name rule counts runes (code points), so the limit check must too: a
  // code-unit length would refuse a limit-length label holding an astral rune.
  const runes = Array.from(label).length
  // A name is unusable when stripping it changes it: it held a rune no
  // identifier may hold, the same class ValidSessionName refuses.
  return runes > 0 && runes <= MAX_SESSION_NAME_RUNES && !RESERVED_SESSION_REFERENCES.has(label) && stripDisallowedSessionNameRunes(label) === label
}

/**
 * The default label for a session working in dir, or an empty string when
 * neither git nor the directory yield anything usable. Both answers come from
 * git itself, so GIT_DIR, linked worktrees and symlinked repositories resolve
 * exactly as git sees them; a missing or unreadable repository falls back to
 * the directory's own base name.
 */
export async function sessionLabel(dir: string | undefined, git: GitReader = defaultGitReader): Promise<string> {
  if (!dir) return ''
  const toplevel = await git(dir, ['rev-parse', '--show-toplevel'])
  if (toplevel) {
    let label = nodePath.basename(toplevel)
    const branch = await git(dir, ['branch', '--show-current'])
    if (branch && !DEFAULT_BRANCH_NAMES.has(branch)) label += '@' + branch
    const candidate = sanitizeSessionLabel(label)
    if (labelUsable(candidate)) return candidate
  }
  const fallback = sanitizeSessionLabel(nodePath.basename(dir))
  return labelUsable(fallback) ? fallback : ''
}

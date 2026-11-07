import { execFile } from 'node:child_process'
import * as nodePath from 'node:path'

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

// The references resolution answers before any label is read: a label spelled
// as one of them could never be reached by name, and the broker refuses it as
// a session name.
const RESERVED_REFERENCES = new Set(['self', 'supervisor'])

// The session display-name limit: 1-64 runes with no control, format, line-
// or paragraph-separator characters.
export const MAX_NAME_RUNES = 64

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
 * a single separator, and the result is truncated to the 64-rune name limit
 * without splitting a rune, then trimmed of separators.
 */
export function sanitizeSessionLabel(label: string): string {
  const stripped = label.replace(/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/gu, '')
  const separated = stripped.replace(/[/\\\s]+/gu, '-')
  const collapsed = separated.replace(/-{2,}/g, '-')
  const truncated = truncateRunes(collapsed, MAX_NAME_RUNES)
  return truncated.replace(/^-+|-+$/g, '')
}

/** Cut label to at most limit runes without splitting one. */
export function truncateRunes(label: string, limit: number): string {
  const runes = Array.from(label)
  return runes.length > limit ? runes.slice(0, limit).join('') : label
}

function labelUsable(label: string): boolean {
  return label.length > 0 && !RESERVED_REFERENCES.has(label)
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

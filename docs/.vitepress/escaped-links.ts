import { statSync } from 'node:fs'
import { dirname, extname, join, normalize } from 'node:path'
import { fileURLToPath } from 'node:url'

export const GITHUB_BASE = 'https://github.com/sdougbrown/avenor'
export const GITHUB_REF = 'main'

// docs/.vitepress/ sits two levels below the repository root.
const REPO_ROOT = fileURLToPath(new URL('../..', import.meta.url))

export function splitEscapes(resolved: string): { depth: number; path: string } {
  let rest = resolved
  let depth = 0
  while (rest.startsWith('../')) {
    rest = rest.slice(3)
    depth++
  }
  if (rest === '..') {
    // A bare trailing `..` has no trailing slash for the loop to consume,
    // but still escapes one more level.
    depth++
    rest = ''
  }
  return { depth, path: rest.replace(/\/$/, '') }
}

// Extension-less files (LICENSE, Makefile, …) need the filesystem to
// distinguish them from directories; for paths that don't exist in the repo,
// fall back to the extension heuristic and let CI's URL check flag the miss.
function linkKind(repoPath: string): 'blob' | 'tree' {
  try {
    return statSync(join(REPO_ROOT, repoPath)).isDirectory() ? 'tree' : 'blob'
  } catch {
    return extname(repoPath) ? 'blob' : 'tree'
  }
}

// Resolve a markdown link against its page's docs-relative path. Returns the
// GitHub URL for a link that escapes the docs root, or null when the link
// stays inside docs/ (VitePress serves those itself). Throws when the link
// escapes the repository. docs/ sits directly at the repo root, so a
// resolvable link escapes by exactly one level; more means it points outside
// the repository.
export function escapedLinkUrl(relativePath: string, href: string): string | null {
  const resolved = normalize(join(dirname(relativePath), href))
  if (resolved !== '..' && !resolved.startsWith('../')) return null
  const { depth, path } = splitEscapes(resolved)
  if (depth > 1) {
    throw new Error(
      `[rewriteEscapedLinks] docs/${relativePath}: link "${href}" resolves to "${resolved}", which escapes the repository. ` +
        'Links may point inside docs/ (handled by VitePress) or one level up to the repo root.',
    )
  }
  const hash = path.indexOf('#')
  const clean = hash === -1 ? path : path.slice(0, hash)
  const fragment = hash === -1 ? '' : path.slice(hash)
  const suffix = clean ? `/${clean}` : ''
  return `${GITHUB_BASE}/${linkKind(clean)}/${GITHUB_REF}${suffix}${fragment}`
}
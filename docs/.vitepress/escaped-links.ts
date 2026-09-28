import { dirname, extname, join, normalize } from 'node:path'

export const GITHUB_BASE = 'https://github.com/sdougbrown/avenor'
export const GITHUB_REF = 'main'

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
  const kind = extname(path) ? 'blob' : 'tree'
  const suffix = path ? `/${path}` : ''
  return `${GITHUB_BASE}/${kind}/${GITHUB_REF}${suffix}`
}
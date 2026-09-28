import { dirname, extname, join, normalize } from 'node:path'
import { defineConfig, type MarkdownIt } from 'vitepress'

// Links that escape the docs source root (e.g. `../templates/foo/README.md`)
// are dead by construction — VitePress only serves pages under docs/. Rewrite
// them to GitHub URLs on the default branch: files get /blob/, extension-less
// paths (directories) get /tree/.
const GITHUB_BASE = 'https://github.com/sdougbrown/avenor'
const GITHUB_REF = 'main'

function splitEscapes(resolved: string): { depth: number; path: string } {
  let rest = resolved
  let depth = 0
  while (rest.startsWith('../')) {
    rest = rest.slice(3)
    depth++
  }
  return { depth, path: rest.replace(/\/$/, '') }
}

function rewriteEscapedLinks(md: MarkdownIt) {
  md.core.ruler.after('inline', 'rewrite-escaped-links', (state) => {
    const relativePath = state.env.relativePath as string | undefined
    if (!relativePath) return false
    const dir = dirname(relativePath)
    for (const token of state.tokens) {
      for (const child of token.children ?? []) {
        if (child.type !== 'link_open') continue
        const href = child.attrGet('href')
        if (!href || !href.startsWith('../')) continue
        const resolved = normalize(join(dir, href))
        if (!resolved.startsWith('../')) continue // stays inside docs/ — leave for VitePress
        // docs/ sits directly at the repo root, so a resolvable link escapes
        // by exactly one level; more means it points outside the repository.
        const { depth, path } = splitEscapes(resolved)
        if (depth > 1) {
          throw new Error(
            `[rewriteEscapedLinks] docs/${relativePath}: link "${href}" resolves to "${resolved}", which escapes the repository. ` +
              'Links may point inside docs/ (handled by VitePress) or one level up to the repo root.',
          )
        }
        const kind = extname(path) ? 'blob' : 'tree'
        const suffix = path ? `/${path}` : ''
        child.attrSet('href', `${GITHUB_BASE}/${kind}/${GITHUB_REF}${suffix}`)
      }
    }
    return false
  })
}

export default defineConfig({
  title: '🏇 Avenor',
  description: 'Agent orchestration harness — dispatch across runtimes, monitor events, handle permissions.',
  base: '/',
  head: [
    ['link', { rel: 'icon', href: '/favicon.svg', type: 'image/svg+xml' }],
    ['link', { rel: 'apple-touch-icon', href: '/apple-touch-icon.png' }],
  ],
  markdown: {
    config: rewriteEscapedLinks,
  },
  themeConfig: {
    appearance: 'dark',
    nav: [
      { text: 'CLI Reference', link: '/cli' },
      { text: 'GitHub', link: 'https://github.com/sdougbrown/avenor' },
    ],
    sidebar: [
      {
        text: 'Getting Started',
        items: [
          { text: 'CLI Reference', link: '/cli' },
          { text: 'Backends', link: '/backends' },
          { text: 'Pony', link: '/pony' },
        ],
      },
      {
        text: 'Running',
        items: [
          { text: 'Loop', link: '/loop' },
          { text: 'Team', link: '/team' },
          { text: 'Workflow', link: '/workflow' },
          { text: 'Workflow Controller', link: '/workflow-controller' },
          { text: 'Events', link: '/events' },
          { text: 'Watch', link: '/watch' },
          { text: 'Permission Handler', link: '/permission-handler' },
        ],
      },
      {
        text: 'Control',
        items: [
          { text: 'Control Protocol', link: '/control-protocol' },
          { text: 'Run Introspection', link: '/introspection' },
          { text: 'Stable', link: '/stable' },
        ],
      },
      {
        text: 'Integration',
        items: [
          { text: 'MCP', link: '/mcp' },
          { text: 'Claude Code Plugin', link: '/claude-plugin' },
          { text: 'Codex Plugin', link: '/codex-plugin' },
        ],
      },
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/sdougbrown/avenor' },
    ],
    editLink: {
      pattern: 'https://github.com/sdougbrown/avenor/edit/main/docs/:path',
      text: 'Edit this page',
    },
  },
})

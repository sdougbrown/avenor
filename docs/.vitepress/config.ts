import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Avenor',
  description: 'Agent orchestration harness — dispatch across runtimes, monitor events, handle permissions.',
  // For a GitHub Pages project site: base: '/avenor/'
  // For a custom domain at the root: base: '/'
  base: '/avenor/',
  themeConfig: {
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
        ],
      },
      {
        text: 'Running',
        items: [
          { text: 'Loop', link: '/loop' },
          { text: 'Events', link: '/events' },
          { text: 'Watch', link: '/watch' },
          { text: 'Permission Handler', link: '/permission-handler' },
        ],
      },
      {
        text: 'Control',
        items: [
          { text: 'Control Protocol', link: '/control-protocol' },
          { text: 'Stable', link: '/stable' },
        ],
      },
      {
        text: 'Integration',
        items: [
          { text: 'MCP', link: '/mcp' },
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

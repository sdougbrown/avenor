import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: { index: 'src/mcp.ts' },
  format: ['esm'],
  dts: true,
  outExtensions: () => ({ js: '.js', dts: '.d.ts' }),
})

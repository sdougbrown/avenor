import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: { index: 'src/plugin.ts' },
  format: ['esm'],
  dts: true,
  fixedExtension: false,
})

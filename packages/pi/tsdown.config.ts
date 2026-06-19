import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: { index: 'src/index.ts' },
  format: ['esm'],
  dts: true,
  fixedExtension: false,
  deps: {
    neverBundle: ['typebox', '@earendil-works/pi-coding-agent', '@earendil-works/pi-tui'],
  },
})

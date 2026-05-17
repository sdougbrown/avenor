import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    postinstall: 'src/postinstall.ts',
  },
  format: ['esm'],
  dts: true,
  fixedExtension: false,
})

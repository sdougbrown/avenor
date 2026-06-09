import { defineConfig } from 'vite'
import solidPlugin from 'vite-plugin-solid'

export default defineConfig({
  plugins: [solidPlugin()],
  build: {
    outDir: 'dist',
  },
  base: '/board/',
  server: {
    proxy: {
      '/api': {
        target: `http://127.0.0.1:${process.env.BOARD_PORT || '8765'}`,
        changeOrigin: true,
      },
    },
  },
})

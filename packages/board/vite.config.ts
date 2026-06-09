import { defineConfig } from 'vite'
import solidPlugin from 'vite-plugin-solid'
import { mockApiPlugin } from './mock'

export default defineConfig({
  plugins: [solidPlugin(), mockApiPlugin()],
  build: {
    outDir: 'dist',
  },
  base: '/board/',
  server: {
    // When BOARD_PORT is set, proxy /api to a real avenor board server
    // instead of using mock data.
    ...(process.env.BOARD_PORT
      ? {
          proxy: {
            '/api': {
              target: `http://127.0.0.1:${process.env.BOARD_PORT}`,
              changeOrigin: true,
            },
          },
        }
      : {}),
  },
})

import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

export default defineConfig({
  plugins: [svelte()],
  server: { proxy: { '/console-api': 'http://127.0.0.1:3002' } },
});

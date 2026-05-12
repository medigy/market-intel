import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';
import tailwindcss from "@tailwindcss/vite";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [
    tsconfigPaths(),
    react(),
    tailwindcss(),
  ],

  build: {
    lib: {
      entry: resolve(__dirname, 'src/web-component/index.ts'),
      name: 'AiChat',
      fileName: 'ai-chat',
      formats: ['es'],         // ES module — works with <script type="module">
    },
    outDir: 'dist/wc',
    rollupOptions: {
      // Do NOT externalize anything — bundle it all into one file
      // so SQLPage only needs one <script> tag
      external: [],
    },
  },

  envDir: '../',
  envPrefix: ['VITE_', 'AI_CHAT_', 'TENENT_ID'],

  // Ensure env vars are handled if needed
  define: {
    'process.env.NODE_ENV': JSON.stringify('production'),
  },

  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
});

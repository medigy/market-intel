import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';
import cssInjectedByJs from 'vite-plugin-css-injected-by-js';
import tailwindcss from "@tailwindcss/vite";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  plugins: [
    tsconfigPaths(),
    react(),
    tailwindcss(),
    // This plugin injects all CSS into the JS bundle automatically
    // so the final output is ONE self-contained .js file
    cssInjectedByJs(),
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

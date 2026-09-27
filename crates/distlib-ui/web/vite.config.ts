/// <reference types="vitest/config" />
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { svelteTesting } from "@testing-library/svelte/vite";
import { defineConfig } from "vite";

// `npm run dev` serves the page with hot reload and forwards the API's two
// routes to a running node — `DISTLIB_API`, or the default `[api] bind_addr`.
// The forwarding happens in the dev server, so the browser sees one origin:
// there is no CORS here to configure, and none should ever be added.
const node = process.env.DISTLIB_API ?? "http://127.0.0.1:11280";

export default defineConfig({
  // `svelteTesting` acts only under Vitest: Svelte's browser build rather
  // than its server one, and the DOM cleaned up after every test.
  plugins: [svelte(), svelteTesting()],
  server: {
    proxy: {
      "/rpc": node,
      "/events": node,
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.ts"],
    setupFiles: ["src/test-setup.ts"],
    // Each test starts from a clean tab: no token, no mocks left over.
    restoreMocks: true,
    unstubGlobals: true,
  },
});

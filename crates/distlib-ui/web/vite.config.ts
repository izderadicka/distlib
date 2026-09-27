import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

// `npm run dev` serves the page with hot reload and forwards the API's two
// routes to a running node — `DISTLIB_API`, or the default `[api] bind_addr`.
// The forwarding happens in the dev server, so the browser sees one origin:
// there is no CORS here to configure, and none should ever be added.
const node = process.env.DISTLIB_API ?? "http://127.0.0.1:11280";

export default defineConfig({
  plugins: [svelte()],
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
});

import { defineConfig } from "@playwright/test";

// The page in a real browser against a real node — what the unit and
// component tests cannot reach: the CSP as a browser enforces it, `fetch`
// streaming as a browser does it, and the node's own serving of the built UI.
export default defineConfig({
  testDir: "e2e",
  // Each test founds a node of its own. One at a time: the suite takes
  // seconds, and nodes picking free ports at once could pick the same one.
  workers: 1,
  timeout: 90_000,
  expect: { timeout: 15_000 },
  forbidOnly: !!process.env.CI,
  reporter: "list",
  use: {
    browserName: "chromium",
    // Into `test-results/`, which CI keeps when a test fails.
    trace: "retain-on-failure",
  },
});

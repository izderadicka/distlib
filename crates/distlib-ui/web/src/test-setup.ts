// Every test starts from a fresh tab: nothing stored, and the address bar —
// and the page's idea of it — at the page's root with no fragment.
import { beforeEach } from "vitest";

import { onPopState } from "./lib/router.svelte";

beforeEach(() => {
  sessionStorage.clear();
  history.replaceState(null, "", "/");
  onPopState();
});

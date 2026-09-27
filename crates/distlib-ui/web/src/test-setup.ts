// Every test starts from a fresh tab: nothing stored, and the address bar at
// the page's root with no fragment.
import { beforeEach } from "vitest";

beforeEach(() => {
  sessionStorage.clear();
  history.replaceState(null, "", "/");
});

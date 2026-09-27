// What the component tests share.
import { vi } from "vitest";

import type { Listen, NodeEvent } from "./lib/events";

/**
 * A stand-in for the page's connection to the node's events: `tell` says
 * something to whoever is listening, and `listening` is how many are.
 */
export function fakeListen() {
  const listeners = new Set<(event: NodeEvent) => void>();
  const listen: Listen = vi.fn((listener) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  });
  return {
    listen,
    tell: (event: NodeEvent) => {
      for (const listener of listeners) {
        listener(event);
      }
    },
    listening: () => listeners.size,
  };
}

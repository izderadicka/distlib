/**
 * Runs `load` whenever asked, but never twice at once: asked while it runs,
 * it runs once more when it is done, however many times it was asked.
 *
 * Events name what changed, one item at a time (D2), so a sync bringing in
 * 250 items is 250 events; a page answering each with its own request would
 * make 250 of them, answered in whatever order the network likes. This makes
 * it at most two, the last of which started after the last event.
 *
 * `load` must not throw: a page shows its own failures.
 */
export function reloader(load: () => Promise<void>): () => void {
  let running = false;
  let again = false;
  return async () => {
    if (running) {
      again = true;
      return;
    }
    running = true;
    try {
      do {
        again = false;
        await load();
      } while (again);
    } finally {
      running = false;
    }
  };
}

// The node's event stream (`GET /events`), kept open for as long as a page
// wants it.
//
// **`fetch`, not `EventSource`** (D3): the browser's own SSE client cannot
// send an `Authorization` header, and the token belongs in one. So the
// stream is read here and its frames parsed by hand.
//
// **Every (re)connection starts with a `resync`.** An event says what
// changed, never what it changed to (D2), and a page answers any of them by
// refetching. Whatever happened while this was not connected — before the
// first connection, or during a gap — is covered by the same refetch, so a
// page loads its data on `resync` and never needs a separate first load that
// could race the subscription.
//
// **A dropped stream is a dropped node, and it is retried** with backoff.
// That is the page's whole liveness signal (D6): `live` while connected,
// `reconnecting` while not, so the page can say its data may be stale. A
// stream that stops carrying anything — even the node's keep-alive comments
// — is taken for dead, the way `distlib download` does it: a connection can
// die without anything closing it.

import { forgetToken, token } from "./token";

/** An event, by §7.2's name. Unknown types pass through; a page ignores them. */
export type NodeEvent =
  | { type: "membership.changed" }
  | { type: "catalogue.item_added"; item_id: string }
  | { type: "catalogue.item_changed"; item_id: string }
  | { type: "resync" }
  | { type: string; [field: string]: unknown };

/** Whether the page is hearing the node. */
export type Connection = "connecting" | "live" | "reconnecting";

export interface Watcher {
  onEvent(event: NodeEvent): void;
  onConnection(connection: Connection): void;
  /** The node refused the token; retrying cannot help. */
  onUnauthorised(): void;
}

/** How often the node sends a keep-alive comment (`events::KEEP_ALIVE`). */
const KEEP_ALIVE_MS = 15_000;
/** Two missed keep-alives are a connection nobody is on the other end of. */
const SILENCE_MS = 2 * KEEP_ALIVE_MS;
const FIRST_RETRY_MS = 1_000;
const LAST_RETRY_MS = 30_000;

/** Watches the node's events until the returned function is called. */
export function watch(watcher: Watcher): () => void {
  let stopped = false;
  let current: AbortController | null = null;

  const run = async () => {
    let retry = FIRST_RETRY_MS;
    watcher.onConnection("connecting");
    while (!stopped) {
      const held = token();
      if (held === null) {
        watcher.onUnauthorised();
        return;
      }
      const controller = new AbortController();
      current = controller;
      try {
        const response = await fetch("/events", {
          headers: { Authorization: `Bearer ${held}` },
          signal: controller.signal,
        });
        if (response.status === 401) {
          forgetToken();
          watcher.onUnauthorised();
          return;
        }
        if (response.ok && response.body) {
          watcher.onConnection("live");
          watcher.onEvent({ type: "resync" });
          retry = FIRST_RETRY_MS;
          await read(response.body, controller, watcher);
        }
      } catch {
        // A refused connection, an abort, a broken stream: all mean the same
        // thing here, and the loop below is the answer to each.
      }
      if (stopped) {
        return;
      }
      watcher.onConnection("reconnecting");
      await new Promise((resolve) => setTimeout(resolve, retry));
      retry = Math.min(retry * 2, LAST_RETRY_MS);
    }
  };
  void run();

  return () => {
    stopped = true;
    current?.abort();
  };
}

/** Reads frames off one connection until it ends, falls silent or is stopped. */
async function read(
  body: ReadableStream<Uint8Array>,
  controller: AbortController,
  watcher: Watcher,
): Promise<void> {
  const reader = body.getReader();
  // `stream: true` holds back a character split across two reads.
  const decoder = new TextDecoder();
  let silence = setTimeout(() => controller.abort(), SILENCE_MS);
  let unread = "";
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) {
        return;
      }
      clearTimeout(silence);
      silence = setTimeout(() => controller.abort(), SILENCE_MS);
      unread += decoder.decode(value, { stream: true });
      let end: number;
      while ((end = unread.indexOf("\n\n")) >= 0) {
        const event = parseFrame(unread.slice(0, end));
        unread = unread.slice(end + 2);
        if (event) {
          watcher.onEvent(event);
        }
      }
    }
  } finally {
    clearTimeout(silence);
  }
}

/**
 * One server-sent event as `distlib-api` writes it: an `event:` line and one
 * `data:` line of JSON. `null` for anything else — a keep-alive comment, or
 * data this page cannot read.
 */
export function parseFrame(frame: string): NodeEvent | null {
  const field = (name: string) =>
    frame
      .split("\n")
      .find((line) => line.startsWith(`${name}:`))
      ?.slice(name.length + 1)
      .trimStart();
  const name = field("event");
  if (name === undefined) {
    return null;
  }
  if (name === "resync") {
    return { type: "resync" };
  }
  try {
    return JSON.parse(field("data") ?? "") as NodeEvent;
  } catch {
    return null;
  }
}

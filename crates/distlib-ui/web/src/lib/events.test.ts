import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { parseFrame, watch } from "./events";
import { token } from "./token";

const encoder = new TextEncoder();

/** One `GET /events` the fake node is holding open. */
interface Connection {
  push(text: string): void;
  pushBytes(bytes: Uint8Array): void;
  close(): void;
  aborted: boolean;
}

/**
 * A node that answers `GET /events` as the test says: by default with a
 * stream the test writes to, or — queued in `next` — a refused connection or
 * an HTTP status.
 */
function fakeNode() {
  const connections: Connection[] = [];
  const next: Array<"refuse" | number> = [];
  const fetch = vi.fn(async (_path: string, init: RequestInit) => {
    const answer = next.shift();
    if (answer === "refuse") {
      throw new TypeError("connection refused");
    }
    if (typeof answer === "number") {
      return new Response("{}", { status: answer });
    }
    let stream!: ReadableStreamDefaultController<Uint8Array>;
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        stream = controller;
      },
    });
    const connection: Connection = {
      aborted: false,
      push: (text) => stream.enqueue(encoder.encode(text)),
      pushBytes: (bytes) => stream.enqueue(bytes),
      close: () => stream.close(),
    };
    init.signal?.addEventListener("abort", () => {
      connection.aborted = true;
      stream.error(new DOMException("aborted", "AbortError"));
    });
    connections.push(connection);
    return new Response(body, { status: 200 });
  });
  vi.stubGlobal("fetch", fetch);
  return { fetch, connections, next };
}

/** Starts a watch that writes down everything it is told. */
function watching() {
  const heard: string[] = [];
  const stop = watch({
    onEvent: (event) =>
      heard.push("item_id" in event ? `${event.type} ${String(event.item_id)}` : event.type),
    onConnection: (connection) => heard.push(`[${connection}]`),
    onUnauthorised: () => heard.push("unauthorised"),
  });
  return { heard, stop };
}

/** Lets everything already scheduled run, without moving the clock. */
const settle = () => vi.advanceTimersByTimeAsync(0);

const membershipChanged = 'event: membership.changed\ndata: {"type":"membership.changed"}\n\n';

describe("watching the node", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    sessionStorage.setItem("distlib.token", "secret");
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts every connection with a resync, then passes on what the node says", async () => {
    const node = fakeNode();
    const { heard } = watching();
    await settle();

    expect(heard).toEqual(["[connecting]", "[live]", "resync"]);
    const [, init] = node.fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer secret");

    node.connections[0].push(membershipChanged);
    await settle();
    expect(heard.at(-1)).toBe("membership.changed");
  });

  it("puts together a frame split across reads, and a character split across its bytes", async () => {
    const node = fakeNode();
    const { heard } = watching();
    await settle();

    const frame = encoder.encode(
      'event: catalogue.item_added\ndata: {"type":"catalogue.item_added","item_id":"Čapek"}\n\n',
    );
    // Inside the two bytes of `Č`.
    const split = frame.indexOf(0xc4) + 1;
    node.connections[0].pushBytes(frame.slice(0, split));
    await settle();
    expect(heard).toEqual(["[connecting]", "[live]", "resync"]);

    node.connections[0].pushBytes(frame.slice(split));
    await settle();
    expect(heard.at(-1)).toBe("catalogue.item_added Čapek");
  });

  it("hears a keep-alive as the node being there, not as news", async () => {
    const node = fakeNode();
    const { heard } = watching();
    await settle();

    node.connections[0].push(":\n\n");
    await settle();

    expect(heard).toEqual(["[connecting]", "[live]", "resync"]);
  });

  it("retries a dropped stream, backing off, and resyncs when it is back", async () => {
    const node = fakeNode();
    const { heard } = watching();
    await settle();

    node.next.push("refuse", "refuse");
    node.connections[0].close();
    await settle();
    expect(heard.at(-1)).toBe("[reconnecting]");

    // One second, then two, then four.
    for (const wait of [1_000, 2_000, 4_000]) {
      const asked = node.fetch.mock.calls.length;
      await vi.advanceTimersByTimeAsync(wait - 1);
      expect(node.fetch).toHaveBeenCalledTimes(asked);
      await vi.advanceTimersByTimeAsync(1);
      expect(node.fetch).toHaveBeenCalledTimes(asked + 1);
    }
    expect(heard.slice(-2)).toEqual(["[live]", "resync"]);

    // Back to a second once it has been connected again.
    node.connections[1].close();
    await settle();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(node.fetch).toHaveBeenCalledTimes(5);
  });

  it("stops backing off further at thirty seconds", async () => {
    const node = fakeNode();
    watching();
    await settle();

    node.next.push(...Array<"refuse">(8).fill("refuse"));
    node.connections[0].close();
    await settle();
    // 1 + 2 + 4 + 8 + 16 seconds, then thirty at a time.
    await vi.advanceTimersByTimeAsync(31_000);
    expect(node.fetch).toHaveBeenCalledTimes(6);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(node.fetch).toHaveBeenCalledTimes(6);
    await vi.advanceTimersByTimeAsync(1);
    expect(node.fetch).toHaveBeenCalledTimes(7);
    await vi.advanceTimersByTimeAsync(30_000);
    expect(node.fetch).toHaveBeenCalledTimes(8);
  });

  it("takes a stream that says nothing for thirty seconds for dead", async () => {
    const node = fakeNode();
    const { heard } = watching();
    await settle();

    await vi.advanceTimersByTimeAsync(29_999);
    expect(node.connections[0].aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    expect(node.connections[0].aborted).toBe(true);
    expect(heard.at(-1)).toBe("[reconnecting]");
  });

  it("keeps a quiet stream that still sends keep-alives", async () => {
    const node = fakeNode();
    watching();
    await settle();

    await vi.advanceTimersByTimeAsync(29_000);
    node.connections[0].push(":\n\n");
    await vi.advanceTimersByTimeAsync(29_000);

    expect(node.connections[0].aborted).toBe(false);
  });

  it("gives up for good when the node refuses the token", async () => {
    const node = fakeNode();
    node.next.push(401);
    const { heard } = watching();
    await settle();
    await vi.advanceTimersByTimeAsync(60_000);

    // Once, and without ever claiming to be reconnecting: retrying cannot
    // help, so it is not attempted.
    expect(heard).toEqual(["[connecting]", "unauthorised"]);
    expect(token()).toBeNull();
    expect(node.fetch).toHaveBeenCalledOnce();
  });

  it("closes the stream when stopped, and does not come back", async () => {
    const node = fakeNode();
    const { heard, stop } = watching();
    await settle();

    stop();
    await vi.advanceTimersByTimeAsync(60_000);

    expect(node.connections[0].aborted).toBe(true);
    expect(node.fetch).toHaveBeenCalledOnce();
    expect(heard).not.toContain("[reconnecting]");
  });
});

describe("reading a frame", () => {
  it("reads an event from its data", () => {
    expect(
      parseFrame('event: catalogue.item_changed\ndata: {"type":"catalogue.item_changed","item_id":"x"}'),
    ).toEqual({ type: "catalogue.item_changed", item_id: "x" });
  });

  it("reads resync by its name alone", () => {
    expect(parseFrame("event: resync\ndata: {}")).toEqual({ type: "resync" });
  });

  it("passes on an event type it has never heard of, for the page to ignore", () => {
    expect(parseFrame('event: wish.changed\ndata: {"type":"wish.changed"}')).toEqual({
      type: "wish.changed",
    });
  });

  it("makes nothing of a keep-alive or of data it cannot read", () => {
    expect(parseFrame(":")).toBeNull();
    expect(parseFrame("event: membership.changed\ndata: {not json")).toBeNull();
  });
});

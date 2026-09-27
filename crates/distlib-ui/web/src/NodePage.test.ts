import { render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NodePage from "./NodePage.svelte";
import { type Watcher, watch } from "./lib/events";
import { call, type Members, type NodeStatus, RpcError, Unauthorised } from "./lib/rpc";

vi.mock("./lib/events", () => ({ watch: vi.fn() }));
vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const CAROL = "c".repeat(64);
const DAVE = "d".repeat(64);

const STATUS: NodeStatus = {
  member: ME,
  group: "b".repeat(64),
  core: true,
  members: 2,
  core_group: [ME],
  changed_at: 4,
  raft: "Leader",
  leader: ME,
  followed_upto: null,
  pending: 1,
};

function members(...extra: Members["members"]): Members {
  return {
    group: STATUS.group,
    changed_at: 4,
    members: [
      // A founder names nobody, themselves included.
      { member: ME, name: "", pledge_bytes: 1_500_000_000, core: true },
      { member: CAROL, name: "carol", pledge_bytes: 0, core: false },
      ...extra,
    ],
  };
}

/** What the node answers with, while the test lets it. */
let answer: { status: NodeStatus; members: Members };

/** Renders the page, and hands back what it watches the node with. */
function open() {
  const stop = vi.fn();
  let watcher!: Watcher;
  vi.mocked(watch).mockImplementation((given) => {
    watcher = given;
    return stop;
  });
  const onUnauthorised = vi.fn();
  const page = render(NodePage, { onUnauthorised });
  return { watcher, stop, onUnauthorised, page };
}

/**
 * Matches a `tag` whose whole text, however many pieces it was rendered in,
 * is `text`.
 */
function wholly(tag: string, text: string) {
  return (_: string, element: Element | null) =>
    element?.tagName.toLowerCase() === tag &&
    element.textContent?.replace(/\s+/g, " ").trim() === text;
}

/** The stream coming up, which starts with a resync. */
function goLive(watcher: Watcher) {
  watcher.onConnection("live");
  watcher.onEvent({ type: "resync" });
}

describe("the node page", () => {
  beforeEach(() => {
    answer = { status: STATUS, members: members() };
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "node.status" ? answer.status : answer.members) as typeof call);
  });

  it("asks for nothing until the stream is up, and says it is connecting", () => {
    open();

    expect(screen.getByText("Connecting…")).toBeTruthy();
    expect(call).not.toHaveBeenCalled();
  });

  it("loads on the resync a connection starts with, and shows the node and its members", async () => {
    const { watcher } = open();
    goLive(watcher);

    await screen.findByText(ME);
    expect(screen.getByText("Live")).toBeTruthy();
    expect(screen.getByText(STATUS.group ?? "")).toBeTruthy();
    expect(screen.getByText(wholly("dd", "core — leader"))).toBeTruthy();
    expect(screen.getByText("1 proposal awaiting approval")).toBeTruthy();

    const rows = screen.getAllByRole("row").slice(1).map((row) => row.textContent);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toContain("no name");
    expect(rows[0]).toContain("core (leader)");
    expect(rows[0]).toContain("1.5 GB");
    expect(rows[1]).toContain("carol");
    expect(rows[1]).toContain("follower");
    expect(rows[1]).toContain("0 B");
  });

  it("loads again when the membership changes", async () => {
    const { watcher } = open();
    goLive(watcher);
    await screen.findByText("carol");

    answer = { status: STATUS, members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }) };
    watcher.onEvent({ type: "membership.changed" });

    await screen.findByText("dave");
  });

  it("does not load again for news that is not about the membership", async () => {
    const { watcher } = open();
    goLive(watcher);
    await screen.findByText("carol");
    const asked = vi.mocked(call).mock.calls.length;

    watcher.onEvent({ type: "catalogue.item_added", item_id: "x" });

    expect(call).toHaveBeenCalledTimes(asked);
  });

  it("says when what it shows may be out of date", async () => {
    const { watcher } = open();
    goLive(watcher);
    await screen.findByText("carol");

    watcher.onConnection("reconnecting");

    await screen.findByText(/Cannot reach the node/);
    // What it had is still shown, marked as possibly stale rather than blanked.
    expect(screen.getByText("carol")).toBeTruthy();
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { watcher, onUnauthorised } = open();

    goLive(watcher);

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });

  it("shows any other failure", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32000, "the read model is not ready"));
    const { watcher } = open();

    goLive(watcher);

    await screen.findByText("the read model is not ready");
  });

  it("stops watching when it is closed", () => {
    const { stop, page } = open();

    page.unmount();

    expect(stop).toHaveBeenCalled();
  });
});

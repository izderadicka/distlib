import { render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NodePage from "./NodePage.svelte";
import { call, type Members, type NodeStatus, RpcError, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

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

/** Renders the page, and hands back what it hears the node through. */
function open() {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(NodePage, { listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
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

describe("the node page", () => {
  beforeEach(() => {
    answer = { status: STATUS, members: members() };
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "node.status" ? answer.status : answer.members) as typeof call);
  });

  it("loads when it opens, and shows the node and its members", async () => {
    open();

    await screen.findByText(ME);
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
    const { events } = open();
    await screen.findByText("carol");

    answer = { status: STATUS, members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }) };
    events.tell({ type: "membership.changed" });

    await screen.findByText("dave");
  });

  it("loads again on a resync: whatever was missed is covered by it", async () => {
    const { events } = open();
    await screen.findByText("carol");

    answer = { status: STATUS, members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }) };
    events.tell({ type: "resync" });

    await screen.findByText("dave");
  });

  it("does not load again for news that is not about the membership", async () => {
    const { events } = open();
    await screen.findByText("carol");
    const asked = vi.mocked(call).mock.calls.length;

    events.tell({ type: "catalogue.item_added", item_id: "x" });

    expect(call).toHaveBeenCalledTimes(asked);
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });

  it("shows any other failure", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32000, "the read model is not ready"));
    open();

    await screen.findByText("the read model is not ready");
  });

  it("stops listening when it is closed", () => {
    const { events, page } = open();
    expect(events.listening()).toBe(1);

    page.unmount();

    expect(events.listening()).toBe(0);
  });
});

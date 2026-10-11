import { render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import WishesPage from "./WishesPage.svelte";
import { call, RpcError, Unauthorised, type WishRecord } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const ITEM = "5".repeat(64);

function wish(id: string, title: string, fulfilledWith: string | null = null): WishRecord {
  return {
    wish_id: id.repeat(32),
    creator: BOB,
    kind: null,
    title,
    authors: null,
    description: null,
    created_at: null,
    status: fulfilledWith === null ? "open" : "fulfilled",
    fulfilled_by: fulfilledWith === null ? [] : [{ member: BOB, item_id: fulfilledWith, title: null }],
    comments: [],
  };
}

/** What the node answers `community.wish_list` with, while the test lets it. */
let wishes: WishRecord[];
/** Bob's name, as the group has it. */
let bobsName: string;

/** The node, as the page asks it: the wishes, who this node is, and the group. */
async function node(method: string) {
  switch (method) {
    case "node.status":
      return { member: ME };
    case "group.members":
      return { group: "g", changed_at: 1, members: [{ member: BOB, name: bobsName, pledge_bytes: 0, core: false }] };
    default:
      return { wishes };
  }
}

/** How many times the page has asked the node for `method`. */
const asked = (method: string) => vi.mocked(call).mock.calls.filter(([called]) => called === method).length;

function open() {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(WishesPage, { listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
}

describe("the wishes page", () => {
  beforeEach(() => {
    wishes = [wish("2", "Krakatit", ITEM), wish("1", "Hordubal")];
    bobsName = "bob";
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation(node as typeof call);
  });

  it("shows every wish in the order the node gives, by whoever made it", async () => {
    open();

    await screen.findByRole("heading", { name: "Krakatit" });
    expect(screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent)).toEqual([
      "Make a wish",
      "Krakatit",
      "Hordubal",
    ]);
    await waitFor(() => expect(screen.getAllByText("bob").length).toBeGreaterThan(0));
  });

  it("says when nobody has wished for anything", async () => {
    wishes = [];
    open();

    await screen.findByText("Nobody has wished for anything yet.");
  });

  it("loads again on news of any wish, or a resync", async () => {
    const { events } = open();
    await screen.findByRole("heading", { name: "Krakatit" });

    for (const event of [{ type: "wish.changed", wish_id: "9".repeat(32) }, { type: "resync" }]) {
      const before = asked("community.wish_list");
      events.tell(event);
      await waitFor(() => expect(asked("community.wish_list")).toBe(before + 1));
    }
  });

  it("loads again on news of an item a wish is fulfilled with, for its title", async () => {
    const { events } = open();
    await screen.findByRole("heading", { name: "Krakatit" });

    wishes = [{ ...wish("2", "Krakatit"), fulfilled_by: [{ member: BOB, item_id: ITEM, title: "Krakatit (audio)" }] }];
    events.tell({ type: "catalogue.item_added", item_id: ITEM });

    await screen.findByRole("link", { name: "Krakatit (audio)" });
  });

  it("does not load again for news of any other item, or of members", async () => {
    const { events } = open();
    await screen.findByRole("heading", { name: "Krakatit" });

    events.tell({ type: "catalogue.item_changed", item_id: "6".repeat(64) });
    events.tell({ type: "availability.changed", member_id: BOB });
    events.tell({ type: "membership.changed" });

    expect(asked("community.wish_list")).toBe(1);
  });

  it("finds members' names again when the membership changes", async () => {
    const { events } = open();
    await waitFor(() => expect(screen.getAllByText("bob").length).toBeGreaterThan(0));

    bobsName = "robert";
    events.tell({ type: "membership.changed" });

    await waitFor(() => expect(screen.getAllByText("robert").length).toBeGreaterThan(0));
  });

  it("says when the node cannot list them", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32603, "catalogue is not open yet"));
    open();

    await screen.findByText("catalogue is not open yet");
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });

  it("stops listening when it is closed", () => {
    const { events, page } = open();
    expect(events.listening()).toBe(1);

    page.unmount();

    expect(events.listening()).toBe(0);
  });
});

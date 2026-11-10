import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import WishesPage from "./WishesPage.svelte";
import { navigate, router } from "./lib/router.svelte";
import { call, RpcError, Unauthorised, type WishRecord } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const ITEM = "5".repeat(64);
/** When each wish was made, in microseconds since the epoch, and as the page writes it. */
const CREATED = 1790521054348270;
const CREATED_ON = new Date(CREATED / 1000).toLocaleDateString();

function wish(id: string, title: string, fulfilledWith: string | null = null): WishRecord {
  return {
    wish_id: id.repeat(64),
    creator: BOB,
    kind: null,
    title,
    authors: null,
    description: null,
    created_at: CREATED,
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

/** Opens the list, or with `id`, one wish. */
function open(id: string | null = null) {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(WishesPage, { id, listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
}

/** The table's rows, header left out, as their cells' text. */
const cells = () =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => [...row.querySelectorAll("td")].map((cell) => cell.textContent?.replace(/\s+/g, " ").trim()));

describe("the wishes page", () => {
  beforeEach(() => {
    wishes = [wish("2", "Krakatit", ITEM), wish("1", "Hordubal")];
    bobsName = "bob";
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation(node as typeof call);
  });

  it("lists every wish in the order the node gives, with who made it, when, and what fulfils it", async () => {
    wishes = [{ ...wish("2", "Krakatit", ITEM), kind: "audiobook" }, wish("1", "Hordubal")];
    open();

    await waitFor(() => expect(cells()[0][3]).toBe("bob"));
    expect(cells()).toEqual([
      ["Krakatit", "audiobook", "fulfilled", "bob", CREATED_ON, "no title"],
      ["Hordubal", "", "open", "bob", CREATED_ON, ""],
    ]);
    expect(screen.getByRole("link", { name: "Krakatit" }).getAttribute("href")).toBe(`/wishes/${"2".repeat(64)}`);
    expect(screen.getByRole("link", { name: "no title" }).getAttribute("href")).toBe(`/items/${ITEM}`);
  });

  it("opens the page for making a wish from under the list", async () => {
    navigate("/wishes");
    open();
    await screen.findByRole("link", { name: "Krakatit" });

    await fireEvent.click(screen.getByRole("button", { name: "Make a wish" }));

    expect(router.route).toEqual({ page: "make_wish" });
  });

  it("shows one wish on its own page, with its comments and a way back to the list", async () => {
    open("1".repeat(64));

    await screen.findByRole("heading", { name: "Hordubal" });
    expect(screen.queryByRole("heading", { name: "Krakatit" })).toBeNull();
    expect(screen.queryByRole("table")).toBeNull();
    expect(screen.getByRole("heading", { name: /^Comments/ })).toBeTruthy();
    expect(screen.getByRole("link", { name: "All wishes" }).getAttribute("href")).toBe("/wishes");
  });

  it("says when it knows of no such wish, until it does", async () => {
    const { events } = open("3".repeat(64));
    await screen.findByText("This node knows of no such wish yet.");

    wishes = [...wishes, wish("3", "Válka s mloky")];
    events.tell({ type: "wish.changed", wish_id: "3".repeat(64) });

    await screen.findByRole("heading", { name: "Válka s mloky" });
  });

  it("says when nobody has wished for anything", async () => {
    wishes = [];
    open();

    await screen.findByText("Nobody has wished for anything yet.");
  });

  it("loads again on news of any wish, or a resync", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Krakatit" });

    for (const event of [{ type: "wish.changed", wish_id: "9".repeat(64) }, { type: "resync" }]) {
      const before = asked("community.wish_list");
      events.tell(event);
      await waitFor(() => expect(asked("community.wish_list")).toBe(before + 1));
    }
  });

  it("loads again on news of an item a wish is fulfilled with, for its title", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Krakatit" });

    wishes = [{ ...wish("2", "Krakatit"), fulfilled_by: [{ member: BOB, item_id: ITEM, title: "Krakatit (audio)" }] }];
    events.tell({ type: "catalogue.item_added", item_id: ITEM });

    await screen.findByRole("link", { name: "Krakatit (audio)" });
  });

  it("does not load again for news of any other item, or of members", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Krakatit" });

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

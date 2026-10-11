import { fireEvent, render, screen, waitFor, within } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import BookmarksPage from "./BookmarksPage.svelte";
import type { Route } from "./lib/router.svelte";
import { call, type ListedBookmark, RpcError, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

type BookmarksRoute = Extract<Route, { page: "bookmarks" }>;

const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const CAROL = "c".repeat(64);
const STRANGER = "d".repeat(64);
const HORDUBAL = "5".repeat(64);
const MLOCI = "6".repeat(64);

// As a node answers `community.bookmarks`.
const BOBS: ListedBookmark = {
  item_id: HORDUBAL,
  title: "Hordubal",
  member: BOB,
  member_name: "bob",
  bookmark_id: "1".repeat(32),
  position: "p. 12",
  note: "the farm",
  created_at: 1790521054348270,
  last_modified: 1790521054348270,
};
const MINE: ListedBookmark = {
  ...BOBS,
  item_id: MLOCI,
  title: null,
  member: ME,
  member_name: null,
  bookmark_id: "2".repeat(32),
  position: "chapter 3",
  note: "",
};

/** What the node answers `community.bookmarks` with, while the test lets it. */
let bookmarks: ListedBookmark[];
/** Bob's name, as the group has it. */
let bobsName: string;

/** The node, as the page asks it: the bookmarks, who this node is, and the group. */
async function node(method: string) {
  switch (method) {
    case "node.status":
      return { member: ME };
    case "group.members":
      return {
        group: "g",
        changed_at: 1,
        members: [
          { member: ME, name: "alice", pledge_bytes: 0, core: true },
          { member: CAROL, name: "carol", pledge_bytes: 0, core: false },
          { member: BOB, name: bobsName, pledge_bytes: 0, core: false },
        ],
      };
    default:
      return { bookmarks };
  }
}

/** How many times the page has asked the node for `method`. */
const asked = (method: string) => vi.mocked(call).mock.calls.filter(([called]) => called === method).length;

function open(route: Partial<BookmarksRoute> = {}) {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(BookmarksPage, {
    route: { page: "bookmarks", query: "", member: null, ...route },
    listen: events.listen,
    onUnauthorised,
  });
  return { events, onUnauthorised, page };
}

const memberSelect = () => screen.getByLabelText("Member") as HTMLSelectElement;
const options = () => [...memberSelect().options].map((option) => option.textContent);
const cells = (text: string) =>
  within(screen.getByText(text).closest("tr") as HTMLElement)
    .getAllByRole("cell")
    .map((cell) => cell.textContent?.trim());

describe("the bookmarks page", () => {
  beforeEach(() => {
    bookmarks = [BOBS, MINE];
    bobsName = "bob";
    history.replaceState(null, "", "/bookmarks");
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation(node as typeof call);
  });

  it("lists every member's bookmarks, each with its item and who left it", async () => {
    open();

    await screen.findByRole("link", { name: "Hordubal" });
    await waitFor(() => expect(cells("p. 12")[1]).toBe("bob"));
    expect(cells("p. 12")).toEqual(["Hordubal", "bob", "p. 12", "the farm", expect.any(String)]);
    expect(cells("chapter 3").slice(0, 3)).toEqual(["no title", "you", "chapter 3"]);
    expect(screen.getByRole("link", { name: "Hordubal" }).getAttribute("href")).toBe(`/items/${HORDUBAL}`);
    expect(screen.getByRole("link", { name: "no title" }).getAttribute("href")).toBe(`/items/${MLOCI}`);
    expect(call).toHaveBeenCalledWith("community.bookmarks", { q: undefined, member: undefined });
  });

  it("asks the node for what the address searches and filters by", async () => {
    open({ query: "farm", member: BOB });

    await screen.findByRole("link", { name: "Hordubal" });
    expect(call).toHaveBeenCalledWith("community.bookmarks", { q: "farm", member: BOB });
    expect((screen.getByRole("searchbox") as HTMLInputElement).value).toBe("farm");
    await waitFor(() => expect(memberSelect().selectedOptions[0].textContent).toBe("bob"));
  });

  it("offers everybody, this member's own, and each other member by name", async () => {
    open();

    await waitFor(() => expect(options()).toEqual(["Everybody", "Mine", "bob", "carol"]));
    expect(memberSelect().value).toBe("");
  });

  it("offers a member filtered by that the group does not know, by the start of its id", async () => {
    open({ member: STRANGER });

    await waitFor(() => expect(options()).toContain("dddddddd…"));
    expect(memberSelect().selectedOptions[0].textContent).toBe("dddddddd…");
  });

  it("searches for what is typed, keeping the member filtered by", async () => {
    open({ member: BOB });

    await fireEvent.input(screen.getByRole("searchbox"), { target: { value: "the farm" } });
    await fireEvent.click(screen.getByRole("button", { name: "Search" }));

    expect(location.pathname + location.search).toBe(`/bookmarks?q=the+farm&member=${BOB}`);
  });

  it("filters by the member chosen at once, keeping the search", async () => {
    open({ query: "farm" });
    await waitFor(() => expect(options()).toContain("Mine"));

    await fireEvent.change(memberSelect(), { target: { value: ME } });
    expect(location.pathname + location.search).toBe(`/bookmarks?q=farm&member=${ME}`);

    await fireEvent.change(memberSelect(), { target: { value: "" } });
    expect(location.pathname + location.search).toBe("/bookmarks?q=farm");
  });

  it("says when nobody has left a bookmark", async () => {
    bookmarks = [];
    open();
    await screen.findByText("Nobody has left a bookmark yet.");
  });

  it("says when no bookmarks match the search or the member", async () => {
    bookmarks = [];
    open({ member: BOB });
    await screen.findByText("No bookmarks match.");
  });

  it("loads again on news of any bookmark, of the membership, or a resync", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Hordubal" });

    for (const event of [
      { type: "bookmark.changed", item_id: "7".repeat(64) },
      { type: "membership.changed" },
      { type: "resync" },
    ]) {
      const before = asked("community.bookmarks");
      events.tell(event);
      await waitFor(() => expect(asked("community.bookmarks")).toBe(before + 1));
    }
  });

  it("loads again on news of an item listed, for its title", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Hordubal" });

    bookmarks = [{ ...BOBS, title: "Hordubal (1933)" }];
    events.tell({ type: "catalogue.item_changed", item_id: HORDUBAL });

    await screen.findByRole("link", { name: "Hordubal (1933)" });
  });

  it("does not load again for news of any other item, or of availability", async () => {
    const { events } = open();
    await screen.findByRole("link", { name: "Hordubal" });

    events.tell({ type: "catalogue.item_changed", item_id: "7".repeat(64) });
    events.tell({ type: "availability.changed", member_id: BOB });

    expect(asked("community.bookmarks")).toBe(1);
  });

  it("finds members' names again when the membership changes", async () => {
    const { events } = open();
    await waitFor(() => expect(cells("p. 12")[1]).toBe("bob"));

    bobsName = "robert";
    events.tell({ type: "membership.changed" });

    await waitFor(() => expect(cells("p. 12")[1]).toBe("robert"));
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

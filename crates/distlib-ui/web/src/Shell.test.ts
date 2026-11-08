import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Shell from "./Shell.svelte";
import { type Watcher, watch } from "./lib/events";
import { navigate } from "./lib/router.svelte";
import { call, type Members, type NodeStatus } from "./lib/rpc";

vi.mock("./lib/events", () => ({ watch: vi.fn() }));
vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const STATUS: NodeStatus = {
  member: ME,
  group: null,
  core: true,
  members: 1,
  core_group: [ME],
  changed_at: 0,
  raft: "Leader",
  leader: ME,
  followed_upto: null,
  pending: 0,
  settings: null,
  protocol: null,
  upgrade_required: null,
  custody: null,
  pins: { queued: 0, fetching: 0, failed: 0, waiting_for_room: 0, kept: 0, releasing: 0 },
  storage: {
    limit_bytes: null,
    own_bytes: 0,
    custody_bytes: 0,
    cache_bytes: 0,
    mandatory_over_limit: false,
    last_collection_at: null,
  },
  sync: { neighbours: [], last_sync: [] },
};
const MEMBERS: Members = { group: null, changed_at: 1, members: [] };
/** The group as the node lists it, while the test lets it. */
let members: Members = MEMBERS;
const ALICE: Members = {
  ...MEMBERS,
  members: [{ member: ME, name: "alice", pledge_bytes: 0, required_pledge_bytes: 0, core: true, coverage: null, red: false }],
};

/** Renders the shell, and hands back what it watches the node with. */
function open() {
  const stop = vi.fn();
  let watcher!: Watcher;
  vi.mocked(watch).mockImplementation((given) => {
    watcher = given;
    return stop;
  });
  const onUnauthorised = vi.fn();
  const shell = render(Shell, { onUnauthorised });
  return { watcher, stop, onUnauthorised, shell };
}

const current = () => screen.getAllByRole("link").find((link) => link.getAttribute("aria-current") === "page");

describe("the shell", () => {
  beforeEach(() => {
    members = MEMBERS;
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) => {
      switch (method) {
        case "node.status":
          return STATUS;
        case "group.members":
          return members;
        case "group.suspects":
          return { suspects: [] };
        default:
          return { results: [], total: 0 };
      }
    }) as typeof call);
  });

  it("says whether the node can be heard", async () => {
    const { watcher } = open();
    expect(screen.getByText("Connecting…")).toBeTruthy();

    watcher.onConnection("live");
    await screen.findByText("Live");

    watcher.onConnection("reconnecting");
    await screen.findByText(/Cannot reach the node — what is shown may be out of date/);
  });

  it("keeps what a page shows when the node cannot be heard", async () => {
    navigate("/node");
    const { watcher } = open();
    await screen.findByText(ME);

    watcher.onConnection("reconnecting");

    await screen.findByText(/Cannot reach the node/);
    expect(screen.getByText(ME)).toBeTruthy();
  });

  /** Answers `node.status` with `upgrade_required` set to each of `answers` in turn, the last one after. */
  function upgradeRequiredIn(...answers: NodeStatus["upgrade_required"][]) {
    const others = vi.mocked(call).getMockImplementation();
    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "node.status"
        ? { ...STATUS, upgrade_required: answers.length > 1 ? answers.shift() : answers[0] }
        : others?.(method as "node.status", null)) as typeof call);
  }

  it("says nothing of an upgrade while the node follows the log", async () => {
    open();
    await screen.findByText("The library is empty.");

    expect(screen.queryByText(/Upgrade required/)).toBeNull();
  });

  it("says on every page when the node stopped for an upgrade, and why", async () => {
    upgradeRequiredIn({ reason: "protocol", group: 2, at: 7 });
    open();

    const banner = await screen.findByRole("alert");
    expect(banner.textContent).toMatch(/Upgrade required:\s+the group speaks protocol 2, which this node's build does not/);
    expect(banner.textContent).toMatch(/stopped following the group's log at index 7/);
    navigate("/node");
    await screen.findByText(ME);
    expect(screen.getByRole("alert")).toBe(banner);
  });

  it("shows the banner when the node says it stopped", async () => {
    upgradeRequiredIn(null, { reason: "undecodable", at: 9 });
    const { watcher } = open();
    await screen.findByText("The library is empty.");
    expect(screen.queryByRole("alert")).toBeNull();

    watcher.onEvent({ type: "node.upgrade_required" });

    const banner = await screen.findByRole("alert");
    expect(banner.textContent).toMatch(/the core nodes send log entries this node's build cannot read/);
    expect(banner.textContent).toMatch(/at index 9/);
  });

  it("asks again after a reconnect, which may have missed the event", async () => {
    upgradeRequiredIn(null, { reason: "protocol", group: 2, at: 7 });
    const { watcher } = open();
    await screen.findByText("The library is empty.");
    watcher.onConnection("reconnecting");
    expect(screen.queryByRole("alert")).toBeNull();

    watcher.onConnection("live");

    await screen.findByRole("alert");
  });

  it("names the node in the tab's title", async () => {
    members = ALICE;
    open();

    await waitFor(() => expect(document.title).toBe("alice · distlib"));
  });

  it("names a node without a name by the start of its id, and again once it is given one", async () => {
    const { watcher } = open();
    await waitFor(() => expect(document.title).toBe(`${ME.slice(0, 12)}… · distlib`));

    members = ALICE;
    watcher.onEvent({ type: "membership.changed" });

    await waitFor(() => expect(document.title).toBe("alice · distlib"));
  });

  it("opens on the library", async () => {
    open();

    await screen.findByText("The library is empty.");
    expect(current()?.textContent).toBe("Library");
  });

  it("opens the page the address names", async () => {
    navigate("/node");
    open();

    await screen.findByText(ME);
    expect(current()?.textContent).toBe("Node");
  });

  it("opens an item's page, and another item's as a page of its own", async () => {
    const members = vi.mocked(call).getMockImplementation();
    vi.mocked(call).mockImplementation((async (method: string, params: { item_id: string }) => {
      if (method !== "library.item") {
        return members?.(method as "node.status", null);
      }
      return {
        item_id: params.item_id,
        title: `Item ${params.item_id.slice(0, 1)}`,
        kind: null,
        authors: null,
        genres: null,
        series: null,
        year: null,
        lang: null,
        description: null,
        files: {},
        last_modified: 0,
        added_by: [],
        custodians: null,
        availability: { held: false, providers: 0, online: [] },
        ratings: { average: null, count: 0, mine: null },
        reviews: [],
        bookmarks: [],
      };
    }) as typeof call);
    navigate(`/items/${"a".repeat(64)}`);
    open();
    await screen.findByText("Item a");

    navigate(`/items/${"b".repeat(64)}`);

    await screen.findByText("Item b");
    expect(call).toHaveBeenCalledWith("library.item", { item_id: "b".repeat(64) });
  });

  it("opens the page for adding an item from its link", async () => {
    open();

    await fireEvent.click(screen.getByRole("link", { name: "Add" }));

    await screen.findByRole("heading", { name: "Add an item" });
    expect(current()?.textContent).toBe("Add");
  });

  it("opens the wishes from their link", async () => {
    open();

    await fireEvent.click(screen.getByRole("link", { name: "Wishes" }));

    await screen.findByRole("heading", { name: "Make a wish" });
    expect(current()?.textContent).toBe("Wishes");
  });

  it("opens the bookmarks from their link", async () => {
    open();

    await fireEvent.click(screen.getByRole("link", { name: "Bookmarks" }));

    await screen.findByRole("searchbox", { name: "Search the bookmarks" });
    expect(current()?.textContent).toBe("Bookmarks");
  });

  it("says when the address names no page", async () => {
    navigate("/shelves");
    open();

    expect(screen.getByText("No such page")).toBeTruthy();
    expect(current()).toBeUndefined();
  });

  it("goes to a page when its link is clicked, without leaving", async () => {
    open();
    await screen.findByText("The library is empty.");

    await fireEvent.click(screen.getByRole("link", { name: "Node" }));

    await screen.findByText(ME);
    expect(location.pathname).toBe("/node");
  });

  it("follows the back button", async () => {
    navigate("/node");
    open();
    await screen.findByText(ME);

    history.replaceState(null, "", "/");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await screen.findByText("The library is empty.");
  });

  it("opens a new search as a new page", async () => {
    open();
    await screen.findByText("The library is empty.");

    navigate("/?q=mloci");

    await screen.findByText("Nothing matches “mloci”.");
    expect(call).toHaveBeenLastCalledWith("library.search", { query: "mloci", offset: 0, limit: 20 });
  });

  it("passes what the node says on to the open page", async () => {
    const { watcher } = open();
    await screen.findByText("The library is empty.");

    vi.mocked(call).mockResolvedValue({ results: [], total: 0 });
    watcher.onEvent({ type: "catalogue.item_added", item_id: "x" });

    const searches = () => vi.mocked(call).mock.calls.filter(([method]) => method === "library.list");
    await waitFor(() => expect(searches()).toHaveLength(2));
  });

  it("hands a refused token up to whoever signs the tab in", () => {
    const { watcher, onUnauthorised } = open();

    watcher.onUnauthorised();

    expect(onUnauthorised).toHaveBeenCalled();
  });

  it("stops watching when it is closed", () => {
    const { stop, shell } = open();

    shell.unmount();

    expect(stop).toHaveBeenCalled();
  });
});

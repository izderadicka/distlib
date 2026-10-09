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
  sync: { neighbours: [], last_sync: [] },
};
const MEMBERS: Members = { group: null, changed_at: 1, members: [] };

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
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) => {
      switch (method) {
        case "node.status":
          return STATUS;
        case "group.members":
          return MEMBERS;
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
    vi.mocked(call).mockImplementation((async (_: string, params: { item_id: string }) => ({
      item_id: params.item_id,
      title: `Item ${params.item_id.slice(0, 1)}`,
      kind: null,
      authors: null,
      genres: null,
      series: null,
      year: null,
      lang: null,
      description: null,
      replicas: null,
      files: {},
      last_modified: 0,
      availability: { held: false, providers: 0 },
    })) as typeof call);
    navigate(`/items/${"a".repeat(64)}`);
    open();
    await screen.findByText("Item a");

    navigate(`/items/${"b".repeat(64)}`);

    await screen.findByText("Item b");
    expect(call).toHaveBeenLastCalledWith("library.item", { item_id: "b".repeat(64) });
  });

  it("opens the page for adding an item from its link", async () => {
    open();

    await fireEvent.click(screen.getByRole("link", { name: "Add" }));

    await screen.findByRole("heading", { name: "Add an item" });
    expect(current()?.textContent).toBe("Add");
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

    await waitFor(() => expect(call).toHaveBeenCalledTimes(2));
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

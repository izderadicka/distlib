import { fireEvent, render, screen, within } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import FulfilWish from "./FulfilWish.svelte";
import { call, type ItemSummary, RpcError } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const WISH = "1".repeat(32);

function item(id: string, title: string | null): ItemSummary {
  return {
    item_id: id.repeat(64),
    title,
    kind: "ebook",
    authors: ["Karel Čapek"],
    genres: null,
    series: null,
    year: null,
  };
}

const KRAKATIT = item("a", "Krakatit");
const UNTITLED = item("b", null);

/** What the node answers a search with, while the test lets it. */
let results: ItemSummary[];

function open() {
  render(FulfilWish, { wishId: WISH, title: "Krakatit", onUnauthorised: vi.fn() });
}

const searchbox = () => screen.getByRole("searchbox") as HTMLInputElement;
const rowOf = (text: string) => screen.getByText(text).closest("tr") as HTMLElement;

describe("fulfilling a wish", () => {
  beforeEach(() => {
    results = [KRAKATIT, UNTITLED];
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "library.search" ? { results, total: results.length } : {}) as typeof call);
  });

  it("searches the library for the wish's title at once", async () => {
    open();

    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));

    expect(searchbox().value).toBe("Krakatit");
    expect(call).toHaveBeenCalledWith("library.search", { query: "Krakatit", offset: 0, limit: 10 });
    await screen.findByRole("link", { name: "Krakatit" });
    expect(within(rowOf("no title")).getByRole("button", { name: "Fulfil with this" })).toBeTruthy();
  });

  it("searches again for what is typed", async () => {
    open();
    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));

    await fireEvent.input(searchbox(), { target: { value: " čapek " } });
    await fireEvent.click(screen.getByRole("button", { name: "Search" }));

    expect(call).toHaveBeenLastCalledWith("library.search", { query: "čapek", offset: 0, limit: 10 });
  });

  it("says when nothing matches what was searched for", async () => {
    results = [];
    open();

    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));
    await fireEvent.input(searchbox(), { target: { value: "Krakatit, read" } });

    await screen.findByText("Nothing matches “Krakatit”.");
  });

  it("fulfils the wish with the item chosen, and closes", async () => {
    open();
    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));
    await screen.findByRole("link", { name: "Krakatit" });

    await fireEvent.click(within(rowOf("Krakatit")).getByRole("button", { name: "Fulfil with this" }));

    expect(call).toHaveBeenCalledWith("community.wish_fulfill", { wish_id: WISH, item_id: KRAKATIT.item_id });
    await screen.findByRole("button", { name: "Fulfil…" });
    expect(screen.queryByRole("searchbox")).toBeNull();
  });

  it("stays open, and says why, when the node refuses", async () => {
    open();
    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));
    await screen.findByRole("link", { name: "Krakatit" });
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, "unknown item"));

    await fireEvent.click(within(rowOf("Krakatit")).getByRole("button", { name: "Fulfil with this" }));

    await screen.findByText("unknown item");
    expect(screen.getByRole("searchbox")).toBeTruthy();
  });

  it("closes without fulfilling anything", async () => {
    open();
    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));
    await screen.findByRole("link", { name: "Krakatit" });

    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.queryByRole("searchbox")).toBeNull();
    expect(screen.queryByRole("link", { name: "Krakatit" })).toBeNull();
    expect(call).not.toHaveBeenCalledWith("community.wish_fulfill", expect.anything());
  });
});

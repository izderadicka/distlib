import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import LibraryPage from "./LibraryPage.svelte";
import { router } from "./lib/router.svelte";
import { call, type ItemPage, type ItemSummary, RpcError, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

// As a node answers `library.list`, taken from a running one.
const MLOCI: ItemSummary = {
  authors: ["Karel Čapek"],
  genres: ["satire"],
  item_id: "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0",
  kind: "audiobook",
  series: { index: 1.0, name: "Mloci" },
  title: "Válka s mloky",
  year: 1936,
};
const UNTITLED: ItemSummary = {
  authors: null,
  genres: null,
  item_id: "d0d36315be2bc663e895b2ae7a8a21205921aae069ff783d6125f0a1bd85ea97",
  kind: "ebook",
  series: null,
  title: null,
  year: null,
};

/** `count` items with titles of their own. */
function items(count: number): ItemSummary[] {
  return Array.from({ length: count }, (_, n) => ({
    ...UNTITLED,
    item_id: n.toString(16).padStart(64, "0"),
    title: `Book ${n}`,
  }));
}

/** What the node answers with, while the test lets it. */
let answer: ItemPage;

function open(query = "", number = 1) {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(LibraryPage, {
    route: { page: "library", query, number },
    listen: events.listen,
    onUnauthorised,
  });
  return { events, onUnauthorised, page };
}

/** The page's rows, header left out, as text. */
const rows = () =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => row.textContent);

const link = (name: string) => screen.queryByRole("link", { name })?.getAttribute("href");

describe("the library page", () => {
  beforeEach(() => {
    answer = { results: [MLOCI, UNTITLED], total: 2 };
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async () => answer) as typeof call);
  });

  it("browses the whole library without a query", async () => {
    open();

    await screen.findByText("Válka s mloky");
    expect(call).toHaveBeenCalledWith("library.list", { offset: 0, limit: 20 });
    const [mloci, untitled] = rows();
    expect(mloci).toContain("Karel Čapek");
    expect(mloci).toContain("Mloci #1");
    expect(mloci).toContain("1936");
    expect(mloci).toContain("audiobook");
    expect(untitled).toContain("no title");
    expect(untitled).toContain("ebook");
  });

  it("links each item, titled or not, to its own page", async () => {
    open();

    await screen.findByText("Válka s mloky");
    expect(link("Válka s mloky")).toBe(`/items/${MLOCI.item_id}`);
    expect(link("no title")).toBe(`/items/${UNTITLED.item_id}`);
  });

  it("searches with a query, for the page the address names", async () => {
    answer = { results: items(5), total: 45 };
    open("čapek", 3);

    await screen.findByText("Book 0");
    expect(call).toHaveBeenCalledWith("library.search", { query: "čapek", offset: 40, limit: 20 });
  });

  it("names a series without its place when it has none", async () => {
    answer = { results: [{ ...MLOCI, series: { name: "Mloci" } }], total: 1 };
    open();

    await screen.findByText("Válka s mloky");
    expect(rows()[0]).toContain("Mloci");
    expect(rows()[0]).not.toContain("#");
  });

  it("says where in the results a page is, with a way to the pages either side", async () => {
    answer = { results: items(20), total: 45 };
    open("mloci", 2);

    await screen.findByText(/21–40 of 45/);
    expect(link("Previous")).toBe("/?q=mloci");
    expect(link("Next")).toBe("/?q=mloci&page=3");
  });

  it("offers no page before the first", async () => {
    answer = { results: items(20), total: 45 };
    open();

    await screen.findByText(/1–20 of 45/);
    expect(link("Previous")).toBeUndefined();
    expect(link("Next")).toBe("/?page=2");
  });

  it("offers no page after the last", async () => {
    answer = { results: items(5), total: 45 };
    open("", 3);

    await screen.findByText(/41–45 of 45/);
    expect(link("Previous")).toBe("/?page=2");
    expect(link("Next")).toBeUndefined();
  });

  it("says when the library is empty", async () => {
    answer = { results: [], total: 0 };
    open();

    await screen.findByText("The library is empty.");
  });

  it("says when nothing matches", async () => {
    answer = { results: [], total: 0 };
    open("mloci");

    await screen.findByText("Nothing matches “mloci”.");
  });

  it("says when a page is past the end, and how to get back", async () => {
    answer = { results: [], total: 45 };
    open("", 9);

    await screen.findByText(/There is no page 9/);
    expect(link("Go to the first page")).toBe("/");
  });

  it("goes to the first page of a search when one is submitted", async () => {
    open("", 4);
    await screen.findByText("Válka s mloky");

    await fireEvent.input(screen.getByRole("searchbox"), { target: { value: "  čapek  " } });
    await fireEvent.submit(screen.getByRole("search"));

    expect(router.route).toEqual({ page: "library", query: "čapek", number: 1 });
  });

  it("goes back to browsing when an empty search is submitted", async () => {
    open("mloci");
    await screen.findByText("Válka s mloky");

    await fireEvent.input(screen.getByRole("searchbox"), { target: { value: "" } });
    await fireEvent.submit(screen.getByRole("search"));

    expect(location.pathname + location.search).toBe("/");
  });

  it("loads again when anything in the catalogue changes, or on a resync", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    for (const event of [
      { type: "catalogue.item_added", item_id: "x" },
      { type: "catalogue.item_changed", item_id: "x" },
      { type: "resync" },
    ]) {
      const asked = vi.mocked(call).mock.calls.length;
      events.tell(event);
      await waitFor(() => expect(call).toHaveBeenCalledTimes(asked + 1));
    }
  });

  it("does not load again for news that is not about the catalogue", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    events.tell({ type: "membership.changed" });

    expect(call).toHaveBeenCalledOnce();
  });

  it("answers a burst of news with one load more, not one each", async () => {
    let finish!: () => void;
    const { events } = open();
    await screen.findByText("Válka s mloky");
    vi.mocked(call).mockImplementation((async () => {
      await new Promise<void>((done) => (finish = done));
      return answer;
    }) as typeof call);

    for (let n = 0; n < 50; n += 1) {
      events.tell({ type: "catalogue.item_added", item_id: String(n) });
    }
    finish();
    await waitFor(() => expect(call).toHaveBeenCalledTimes(3));
    finish();
    await new Promise((done) => setTimeout(done, 0));

    expect(call).toHaveBeenCalledTimes(3);
  });

  it("shows why a search could not be run", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, 'could not parse the search query "title:("'));
    open("title:(");

    await screen.findByText('could not parse the search query "title:("');
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

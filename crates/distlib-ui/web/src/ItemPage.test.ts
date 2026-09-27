import { render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import ItemPage from "./ItemPage.svelte";
import { call, type ItemRecord, RpcError, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

// As a node answers `library.item`, taken from a running one.
const MLOCI: ItemRecord = {
  authors: ["Karel Čapek"],
  description: null,
  files: {
    "23ac86d6d127d50c6456fa94bc9018951c425fed59de40993db6b11f0519df2c": {
      filename: "c1.mp3",
      format: "mp3",
      role: "content",
      size: 11,
    },
    "3e85fa08f61f8347fdbb10d2a420c263250c7f5b1e3a7dd4d8d3aae48e8e9c85": {
      filename: "c2.mp3",
      format: "mp3",
      role: "content",
      size: 12,
    },
  },
  genres: ["satire"],
  item_id: "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0",
  kind: "audiobook",
  lang: null,
  last_modified: 1790521054348270,
  replicas: null,
  series: { index: 1.0, name: "Mloci" },
  title: "Válka s mloky",
  year: 1936,
};

/** Everything nobody has said left unsaid, as a node answers it. */
const BARE: ItemRecord = {
  ...MLOCI,
  authors: null,
  genres: null,
  kind: null,
  series: null,
  title: null,
  year: null,
};

const OTHER = "f".repeat(64);

/** What the node answers with, while the test lets it. */
let answer: ItemRecord;

function open(id = MLOCI.item_id) {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(ItemPage, { id, listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
}

/** The item's details, as `term: definition` pairs. */
function details(): Record<string, string> {
  const terms = [...document.querySelectorAll("dt")];
  return Object.fromEntries(terms.map((term) => [term.textContent, term.nextElementSibling?.textContent ?? ""]));
}

/** The files table's rows, header left out, as text. */
const files = () =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => [...row.querySelectorAll("td")].map((cell) => cell.textContent));

describe("the item page", () => {
  beforeEach(() => {
    answer = MLOCI;
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async () => answer) as typeof call);
  });

  it("shows what the node holds for the item", async () => {
    open();

    await screen.findByRole("heading", { name: "Válka s mloky" });
    expect(call).toHaveBeenCalledWith("library.item", { item_id: MLOCI.item_id });
    expect(details()).toMatchObject({
      Authors: "Karel Čapek",
      Series: "Mloci #1",
      Year: "1936",
      Type: "audiobook",
      Genres: "satire",
      Item: MLOCI.item_id,
    });
    expect(document.querySelector("time")?.getAttribute("datetime")).toBe("2026-09-27T14:57:34.348Z");
    expect(files()).toEqual([
      ["c1.mp3", "content", "mp3", "11 B"],
      ["c2.mp3", "content", "mp3", "12 B"],
    ]);
  });

  it("leaves out what nobody has said", async () => {
    answer = BARE;
    open();

    await screen.findByText("no title");
    expect(Object.keys(details())).toEqual(["Changed", "Item"]);
    expect(document.querySelector(".description")).toBeNull();
  });

  it("shows what is said only on an item's own page", async () => {
    answer = { ...MLOCI, lang: "cs", replicas: 3, description: "Satira.\nO mlocích." };
    open();

    await screen.findByText(/Satira\./);
    expect(details()).toMatchObject({ Language: "cs", "Copies kept": "3" });
    expect(document.querySelector(".description")?.textContent).toBe("Satira.\nO mlocích.");
  });

  it("shows a year and a number of copies of nought", async () => {
    answer = { ...MLOCI, year: 0, replicas: 0 };
    open();

    await screen.findByText("Válka s mloky");
    expect(details()).toMatchObject({ Year: "0", "Copies kept": "0" });
  });

  it("names a series without its place when it has none", async () => {
    answer = { ...MLOCI, series: { name: "Mloci" } };
    open();

    await screen.findByText("Válka s mloky");
    expect(details().Series).toBe("Mloci");
  });

  it("lists files in the order they are played: by disc, then place, then name", async () => {
    const file = { format: "mp3", role: "content", size: 1 } as const;
    answer = {
      ...MLOCI,
      files: {
        a: { ...file, filename: "z-first.mp3", disc: 1, seq: 1 },
        b: { ...file, filename: "a-last.mp3", disc: 2, seq: 1 },
        c: { ...file, filename: "m-second.mp3", disc: 1, seq: 2 },
        d: { ...file, filename: "cover.jpg" },
        e: { ...file, filename: "blurb.txt" },
      },
    };
    open();

    await screen.findByText("z-first.mp3");
    expect(files().map(([name]) => name)).toEqual([
      "blurb.txt",
      "cover.jpg",
      "z-first.mp3",
      "m-second.mp3",
      "a-last.mp3",
    ]);
  });

  it("loads again on news of this item, or a resync", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    for (const event of [
      { type: "catalogue.item_changed", item_id: MLOCI.item_id },
      { type: "catalogue.item_added", item_id: MLOCI.item_id },
      { type: "resync" },
    ]) {
      const asked = vi.mocked(call).mock.calls.length;
      events.tell(event);
      await waitFor(() => expect(call).toHaveBeenCalledTimes(asked + 1));
    }
  });

  it("does not load again for news of any other item", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    events.tell({ type: "catalogue.item_changed", item_id: OTHER });
    events.tell({ type: "membership.changed" });

    expect(call).toHaveBeenCalledOnce();
  });

  it("answers a burst of news of it with one load more, not one each", async () => {
    let finish!: () => void;
    const { events } = open();
    await screen.findByText("Válka s mloky");
    vi.mocked(call).mockImplementation((async () => {
      await new Promise<void>((done) => (finish = done));
      return answer;
    }) as typeof call);

    for (let n = 0; n < 10; n += 1) {
      events.tell({ type: "catalogue.item_changed", item_id: MLOCI.item_id });
    }
    finish();
    await waitFor(() => expect(call).toHaveBeenCalledTimes(3));
    finish();
    await new Promise((done) => setTimeout(done, 0));

    expect(call).toHaveBeenCalledTimes(3);
  });

  it("says when the node has no such item, and shows it when it arrives", async () => {
    vi.mocked(call).mockRejectedValueOnce(new RpcError(-32000, `no such item: ${MLOCI.item_id}`));
    const { events } = open();
    await screen.findByText(`no such item: ${MLOCI.item_id}`);

    events.tell({ type: "catalogue.item_added", item_id: MLOCI.item_id });

    await screen.findByText("Válka s mloky");
    expect(screen.queryByText(/no such item/)).toBeNull();
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

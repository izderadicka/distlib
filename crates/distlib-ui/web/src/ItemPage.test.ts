import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
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
  availability: { held: false, providers: 1 },
  ratings: { average: null, count: 0, mine: null },
  reviews: [],
  bookmarks: [],
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
const ME = "a".repeat(64);

/** What the node answers `library.item` with, while the test lets it. */
let answer: ItemRecord;

/** The node, as the page asks it: the item, who this node is, and the group. */
async function node(method: string) {
  switch (method) {
    case "node.status":
      return { member: ME };
    case "group.members":
      return { group: "g", changed_at: 1, members: [{ member: OTHER, name: "bob", pledge_bytes: 0, core: false }] };
    default:
      return answer;
  }
}

/** How many times the page has asked the node for `method`. */
const asked = (method: string) => vi.mocked(call).mock.calls.filter(([called]) => called === method).length;

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
    vi.mocked(call).mockImplementation(node as typeof call);
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
      Availability: "1 online",
      Rating: "not rated yet",
      Item: MLOCI.item_id,
    });
    expect(document.querySelector("time")?.getAttribute("datetime")).toBe("2026-09-27T14:57:34.348Z");
    expect(files()).toEqual([
      ["c1.mp3", "content", "mp3", "11 B"],
      ["c2.mp3", "content", "mp3", "12 B"],
    ]);
    expect(screen.getByRole("button", { name: "Download" })).toBeTruthy();
  });

  it("leaves out what nobody has said", async () => {
    answer = BARE;
    open();

    await screen.findByText("no title");
    expect(Object.keys(details())).toEqual(["Availability", "Rating", "Changed", "Item"]);
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

  it("loads again on news of this item or its bookmarks, or a resync", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    for (const event of [
      { type: "catalogue.item_changed", item_id: MLOCI.item_id },
      { type: "catalogue.item_added", item_id: MLOCI.item_id },
      { type: "bookmark.changed", item_id: MLOCI.item_id },
      { type: "resync" },
    ]) {
      const before = asked("library.item");
      events.tell(event);
      await waitFor(() => expect(asked("library.item")).toBe(before + 1));
    }
  });

  it("names members, and finds their names again when the membership changes", async () => {
    answer = {
      ...MLOCI,
      ratings: { average: 4.5, count: 2, mine: 4 },
      reviews: [{ member: OTHER, review: "Too sad." }],
    };
    const { events } = open();
    await screen.findByRole("heading", { name: "bob" });
    expect(details().Rating).toBe("4.5 from 2 ratings");

    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "group.members" ?
        { group: "g", changed_at: 2, members: [{ member: OTHER, name: "robert", pledge_bytes: 0, core: false }] }
      : node(method)) as typeof call);
    events.tell({ type: "membership.changed" });

    await screen.findByRole("heading", { name: "robert" });
  });

  it("shows a change in who has it when any member's availability changes", async () => {
    const { events } = open();
    await screen.findByText("1 online");

    answer = { ...MLOCI, availability: { held: true, providers: 2 } };
    events.tell({ type: "availability.changed", member_id: OTHER });

    await screen.findByText("held here, 2 online");
  });

  it("does not load again for news of any other item", async () => {
    const { events } = open();
    await screen.findByText("Válka s mloky");

    events.tell({ type: "catalogue.item_changed", item_id: OTHER });
    events.tell({ type: "membership.changed" });

    expect(asked("library.item")).toBe(1);
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
    await waitFor(() => expect(asked("library.item")).toBe(3));
    finish();
    await new Promise((done) => setTimeout(done, 0));

    expect(asked("library.item")).toBe(3);
  });

  it("says when the node has no such item, and shows it when it arrives", async () => {
    vi.mocked(call).mockRejectedValueOnce(new RpcError(-32000, `no such item: ${MLOCI.item_id}`));
    const { events } = open();
    await screen.findByText(`no such item: ${MLOCI.item_id}`);

    events.tell({ type: "catalogue.item_added", item_id: MLOCI.item_id });

    await screen.findByText("Válka s mloky");
    expect(screen.queryByText(/no such item/)).toBeNull();
  });

  it("opens its edit form, and after a save closes it and loads the item again", async () => {
    open();
    await screen.findByText("Válka s mloky");
    await fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    expect(screen.getByRole("heading", { name: "Edit" })).toBeTruthy();

    answer = { ...MLOCI, title: "War with the Newts" };
    await fireEvent.input(screen.getByLabelText(/^Title/), { target: { value: "War with the Newts" } });
    await fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await screen.findByRole("heading", { name: "War with the Newts" });
    expect(screen.queryByRole("heading", { name: "Edit" })).toBeNull();
    expect(call).toHaveBeenCalledWith("library.edit_metadata", {
      item_id: MLOCI.item_id,
      fields: { title: "War with the Newts" },
    });
  });

  it("closes its edit form unchanged when the edit is cancelled", async () => {
    open();
    await screen.findByText("Válka s mloky");
    await fireEvent.click(screen.getByRole("button", { name: "Edit" }));

    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(screen.getByRole("heading", { name: "Válka s mloky" })).toBeTruthy();
    expect(screen.queryByRole("heading", { name: "Edit" })).toBeNull();
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

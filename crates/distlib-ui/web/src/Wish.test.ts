import { fireEvent, render, screen } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Wish from "./Wish.svelte";
import { call, type WishRecord } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const CAROL = "c".repeat(64);
const NAMES = new Map([
  [BOB, "bob"],
  [CAROL, "carol"],
]);
const ITEM = "5".repeat(64);
const OTHER_ITEM = "6".repeat(64);

// As a node answers `community.wish_list`.
const KRAKATIT: WishRecord = {
  wish_id: "1".repeat(32),
  creator: BOB,
  kind: "audiobook",
  title: "Krakatit",
  authors: ["Karel Čapek"],
  description: "The one read by Hrušínský.",
  created_at: 1790521054348270,
  status: "fulfilled",
  fulfilled_by: [
    { member: BOB, item_id: ITEM, title: "Krakatit (audio)" },
    { member: CAROL, item_id: OTHER_ITEM, title: null },
  ],
  comments: [
    { member: ME, comment: "Me too." },
    { member: CAROL, comment: "Try the library\nin Brno." },
  ],
};

/** Everything nobody has said left unsaid. */
const BARE: WishRecord = {
  ...KRAKATIT,
  kind: null,
  authors: null,
  description: null,
  created_at: null,
  status: "open",
  fulfilled_by: [],
  comments: [],
};

function open(wish: WishRecord) {
  render(Wish, { wish, me: ME, names: NAMES, onUnauthorised: vi.fn() });
}

/** The text a `<dt>` term is given. */
const termOf = (term: string) =>
  [...document.querySelectorAll("dt")].find((dt) => dt.textContent === term)?.nextElementSibling?.textContent;

describe("a wish", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ wish_id: KRAKATIT.wish_id });
  });

  it("says what was wished for, and by whom", () => {
    open(KRAKATIT);

    expect(screen.getByRole("heading", { name: "Krakatit" })).toBeTruthy();
    expect(termOf("Status")).toBe("fulfilled");
    expect(termOf("Wished by")).toBe("bob");
    expect(termOf("Type")).toBe("audiobook");
    expect(termOf("Authors")).toBe("Karel Čapek");
    expect(termOf("Wished")).toBeTruthy();
    expect(screen.getByText("The one read by Hrušínský.")).toBeTruthy();
  });

  it("leaves out what nobody said", () => {
    open(BARE);

    expect(termOf("Status")).toBe("open");
    expect([...document.querySelectorAll("dt")].map((dt) => dt.textContent)).toEqual(["Status", "Wished by"]);
    expect(document.querySelector("ul")).toBeNull();
  });

  it("links every item it is fulfilled with, by who said so", () => {
    open(KRAKATIT);

    const fulfilments = [...document.querySelectorAll("li")].map((li) => li.textContent?.replace(/\s+/g, " ").trim());
    expect(fulfilments).toEqual(["bob fulfilled it with Krakatit (audio)", "carol fulfilled it with no title"]);
    expect(screen.getByRole("link", { name: "Krakatit (audio)" }).getAttribute("href")).toBe(`/items/${ITEM}`);
    expect(screen.getByRole("link", { name: "no title" }).getAttribute("href")).toBe(`/items/${OTHER_ITEM}`);
  });

  it("shows everybody else's comments by name, and this member's own in the form", () => {
    open(KRAKATIT);

    expect(screen.getByRole("heading", { name: "Comments 2" })).toBeTruthy();
    expect([...document.querySelectorAll("p.description")].map((p) => p.textContent)).toEqual([
      "The one read by Hrušínský.",
      "carol: Try the library\nin Brno.",
    ]);
    expect((screen.getByLabelText("Your comment") as HTMLTextAreaElement).value).toBe("Me too.");
  });

  it("saves and clears this member's comment", async () => {
    open(KRAKATIT);

    await fireEvent.input(screen.getByLabelText("Your comment"), { target: { value: "Found it." } });
    await fireEvent.click(screen.getByRole("button", { name: "Save comment" }));
    await fireEvent.click(screen.getByRole("button", { name: "Clear comment" }));

    expect(call).toHaveBeenCalledWith("community.wish_comment", { wish_id: KRAKATIT.wish_id, comment: "Found it." });
    await vi.waitFor(() =>
      expect(call).toHaveBeenLastCalledWith("community.wish_comment", { wish_id: KRAKATIT.wish_id, comment: null }),
    );
  });

  it("offers to fulfil it, starting from its title", async () => {
    vi.mocked(call).mockResolvedValue({ results: [], total: 0 });
    open(KRAKATIT);

    await fireEvent.click(screen.getByRole("button", { name: "Fulfil…" }));

    expect(call).toHaveBeenCalledWith("library.search", { query: "Krakatit", offset: 0, limit: 10 });
  });
});

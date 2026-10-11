import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Bookmarks from "./Bookmarks.svelte";
import { type BookmarkRecord, call, RpcError } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ITEM = "5".repeat(64);
const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const NAMES = new Map([[BOB, "bob"]]);

const bookmark = (member: string, id: string, position: string, created_at: number): BookmarkRecord => ({
  member,
  bookmark_id: id.repeat(32),
  position,
  note: `note at ${position}`,
  created_at,
  last_modified: created_at,
});
const MINE = bookmark(ME, "1", "p. 12", 1);
const BOBS = bookmark(BOB, "2", "p. 40", 2);

function open(bookmarks: BookmarkRecord[] = [MINE, BOBS]) {
  return render(Bookmarks, { itemId: ITEM, bookmarks, me: ME, names: NAMES, onUnauthorised: vi.fn() });
}

/** The table's rows, header left out, as their first three cells' text. */
const rows = () =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => [...row.querySelectorAll("td")].slice(0, 3).map((cell) => cell.textContent));

const field = (label: string) => screen.getByLabelText(label) as HTMLInputElement;
const button = (name: string) => screen.getByRole("button", { name }) as HTMLButtonElement;

describe("an item's bookmarks", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ item_id: ITEM, bookmark_id: "9".repeat(32) });
  });

  it("lists every member's, newest first, by name", () => {
    open();

    expect(rows()).toEqual([
      ["bob", "p. 40", "note at p. 40"],
      ["you", "p. 12", "note at p. 12"],
    ]);
  });

  it("offers to edit and delete this member's own only", () => {
    open();

    expect(screen.getByRole("button", { name: "Edit bookmark at p. 12" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "Delete bookmark at p. 12" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Edit bookmark at p. 40" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete bookmark at p. 40" })).toBeNull();
  });

  it("leaves a new one, and empties the form once it is saved", async () => {
    open([]);

    await fireEvent.input(field("Position"), { target: { value: "chapter 3" } });
    await fireEvent.input(field("Note"), { target: { value: "the farm" } });
    await fireEvent.click(button("Add bookmark"));

    expect(call).toHaveBeenCalledWith("community.bookmark", {
      item_id: ITEM,
      position: "chapter 3",
      note: "the farm",
    });
    await waitFor(() => expect(field("Position").value).toBe(""));
    expect(field("Note").value).toBe("");
  });

  it("needs a position", async () => {
    open([]);
    expect(button("Add bookmark").disabled).toBe(true);

    await fireEvent.input(field("Position"), { target: { value: " " } });

    expect(button("Add bookmark").disabled).toBe(true);
  });

  it("edits one of this member's in the form, by its id", async () => {
    open();

    await fireEvent.click(button("Edit bookmark at p. 12"));
    expect(field("Position").value).toBe("p. 12");
    expect(field("Note").value).toBe("note at p. 12");
    await fireEvent.input(field("Position"), { target: { value: "p. 13" } });
    await fireEvent.click(button("Save bookmark"));

    expect(call).toHaveBeenCalledWith("community.bookmark", {
      item_id: ITEM,
      bookmark_id: MINE.bookmark_id,
      position: "p. 13",
      note: "note at p. 12",
    });
    await screen.findByRole("button", { name: "Add bookmark" });
  });

  it("goes back to leaving a new one when an edit is cancelled", async () => {
    open();
    await fireEvent.click(button("Edit bookmark at p. 12"));

    await fireEvent.click(button("Cancel"));

    expect(field("Position").value).toBe("");
    expect(button("Add bookmark")).toBeTruthy();
  });

  it("deletes one of this member's", async () => {
    open();

    await fireEvent.click(button("Delete bookmark at p. 12"));

    expect(call).toHaveBeenCalledWith("community.bookmark_delete", {
      item_id: ITEM,
      bookmark_id: MINE.bookmark_id,
    });
  });

  it("says what the node refused, and keeps what was typed", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, "bookmark note is too long"));
    open([]);

    await fireEvent.input(field("Position"), { target: { value: "p. 1" } });
    await fireEvent.click(button("Add bookmark"));

    await screen.findByText("bookmark note is too long");
    expect(field("Position").value).toBe("p. 1");
  });
});

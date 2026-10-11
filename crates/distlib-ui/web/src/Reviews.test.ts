import { fireEvent, render, screen } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Reviews from "./Reviews.svelte";
import { call, type ReviewRecord, RpcError } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ITEM = "5".repeat(64);
const ME = "a".repeat(64);
const BOB = "b".repeat(64);
const NAMES = new Map([[BOB, "bob"]]);

const MINE: ReviewRecord = { member: ME, review: "Čapek at his quietest." };
const BOBS: ReviewRecord = { member: BOB, review: "Too sad.\nBut true." };

function open(reviews: ReviewRecord[], me: string | null = ME) {
  return render(Reviews, { itemId: ITEM, reviews, me, names: NAMES, onUnauthorised: vi.fn() });
}

const draft = () => screen.getByLabelText("Your review") as HTMLTextAreaElement;
const button = (name: string) => screen.getByRole("button", { name }) as HTMLButtonElement;

describe("an item's reviews", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ item_id: ITEM });
  });

  it("shows everybody else's by name, and this member's own in the form", () => {
    open([MINE, BOBS]);

    expect(screen.getByRole("heading", { name: "bob" })).toBeTruthy();
    expect(document.querySelector(".review p")?.textContent).toBe("Too sad.\nBut true.");
    expect(document.querySelectorAll(".review")).toHaveLength(1);
    expect(draft().value).toBe("Čapek at his quietest.");
  });

  it("saves this member's review", async () => {
    open([BOBS]);

    await fireEvent.input(draft(), { target: { value: "A quiet book." } });
    await fireEvent.click(button("Save review"));

    expect(call).toHaveBeenCalledWith("community.review", { item_id: ITEM, review: "A quiet book." });
  });

  it("has nothing to save while the form is blank or unchanged", async () => {
    open([MINE]);
    expect(button("Save review").disabled).toBe(true);

    await fireEvent.input(draft(), { target: { value: "  " } });

    expect(button("Save review").disabled).toBe(true);
  });

  it("clears this member's review, and offers to only when there is one", async () => {
    open([MINE]);

    await fireEvent.click(button("Clear review"));

    expect(call).toHaveBeenCalledWith("community.review", { item_id: ITEM, review: null });
  });

  it("offers no clearing without a review of this member's", () => {
    open([BOBS]);

    expect(screen.queryByRole("button", { name: "Clear review" })).toBeNull();
  });

  it("fills the form once it is known which review is this member's", async () => {
    const page = open([MINE, BOBS], null);
    expect(draft().value).toBe("");

    await page.rerender({ me: ME });

    expect(draft().value).toBe("Čapek at his quietest.");
  });

  it("says what the node refused", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, "review is too long"));
    open([]);

    await fireEvent.input(draft(), { target: { value: "Long." } });
    await fireEvent.click(button("Save review"));

    await screen.findByText("review is too long");
  });
});

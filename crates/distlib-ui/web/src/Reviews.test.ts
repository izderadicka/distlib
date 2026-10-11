import { fireEvent, render, screen } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Reviews from "./Reviews.svelte";
import { call, type ReviewRecord } from "./lib/rpc";

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
const button = (name: string) => screen.getByRole("button", { name });

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

  it("clears this member's review", async () => {
    open([MINE]);

    await fireEvent.click(button("Clear review"));

    expect(call).toHaveBeenCalledWith("community.review", { item_id: ITEM, review: null });
  });

  it("fills the form once it is known which review is this member's", async () => {
    const page = open([MINE, BOBS], null);
    expect(draft().value).toBe("");

    await page.rerender({ me: ME });

    expect(draft().value).toBe("Čapek at his quietest.");
  });
});

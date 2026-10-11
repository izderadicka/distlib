import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import StarRating from "./StarRating.svelte";
import { call, type Ratings, RpcError, Unauthorised } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ITEM = "5".repeat(64);

function open(ratings: Ratings) {
  const onUnauthorised = vi.fn();
  render(StarRating, { itemId: ITEM, ratings, onUnauthorised });
  return { onUnauthorised };
}

const star = (count: number) => screen.getByRole("button", { name: count === 1 ? "1 star" : `${count} stars` });
const stars = () => [1, 2, 3, 4, 5].map((count) => star(count).textContent).join("");

describe("rating an item", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ item_id: ITEM });
  });

  it("fills the stars up to this member's rating, and marks it", () => {
    open({ average: 4.5, count: 2, mine: 4 });

    expect(stars()).toBe("★★★★☆");
    expect(star(4).getAttribute("aria-pressed")).toBe("true");
    expect(star(5).getAttribute("aria-pressed")).toBe("false");
  });

  it("leaves every star empty, and nothing to clear, before this member has rated it", () => {
    open({ average: 4, count: 1, mine: null });

    expect(stars()).toBe("☆☆☆☆☆");
    expect((screen.getByRole("button", { name: "Clear rating" }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("saves a rating with a click on a star", async () => {
    open({ average: null, count: 0, mine: null });

    await fireEvent.click(star(3));

    expect(call).toHaveBeenCalledWith("community.rate", { item_id: ITEM, rating: 3 });
  });

  it("clears this member's rating", async () => {
    open({ average: 4, count: 1, mine: 4 });

    await fireEvent.click(screen.getByRole("button", { name: "Clear rating" }));

    expect(call).toHaveBeenCalledWith("community.rate", { item_id: ITEM, rating: null });
  });

  it("says what the node refused", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32000, `no such item: ${ITEM}`));
    open({ average: null, count: 0, mine: null });

    await fireEvent.click(star(5));

    await screen.findByText(`no such item: ${ITEM}`);
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open({ average: null, count: 0, mine: null });

    await fireEvent.click(star(2));

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });
});

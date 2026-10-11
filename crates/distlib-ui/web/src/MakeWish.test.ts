import { fireEvent, render, screen } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import MakeWish from "./MakeWish.svelte";
import { call, RpcError } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const field = (label: string | RegExp) => screen.getByLabelText(label) as HTMLInputElement;
const button = () => screen.getByRole("button", { name: "Wish for it" }) as HTMLButtonElement;

async function fill(label: string | RegExp, value: string) {
  await fireEvent.input(field(label), { target: { value } });
}

describe("making a wish", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ wish_id: "1".repeat(32) });
  });

  it("wishes for an item by everything said about it", async () => {
    render(MakeWish, { onUnauthorised: vi.fn() });

    await fill("Title", " Krakatit ");
    await fireEvent.change(field("Type"), { target: { value: "audiobook" } });
    await fill(/^Authors/, "Karel Čapek\n\n Josef Čapek ");
    await fill("Description", "The one read by Hrušínský.");
    await fireEvent.click(button());

    expect(call).toHaveBeenCalledWith("community.wish_create", {
      title: "Krakatit",
      kind: "audiobook",
      authors: ["Karel Čapek", "Josef Čapek"],
      description: "The one read by Hrušínský.",
    });
  });

  it("leaves out what is not said", async () => {
    render(MakeWish, { onUnauthorised: vi.fn() });

    await fill("Title", "Krakatit");
    await fireEvent.click(button());

    expect(call).toHaveBeenCalledWith("community.wish_create", {
      title: "Krakatit",
      kind: undefined,
      authors: undefined,
      description: undefined,
    });
  });

  it("needs a title", async () => {
    render(MakeWish, { onUnauthorised: vi.fn() });
    expect(button().disabled).toBe(true);

    await fill("Title", "  ");

    expect(button().disabled).toBe(true);
  });

  it("empties the form once the wish is made", async () => {
    render(MakeWish, { onUnauthorised: vi.fn() });

    await fill("Title", "Krakatit");
    await fill(/^Authors/, "Karel Čapek");
    await fireEvent.click(button());

    await vi.waitFor(() => expect(field("Title").value).toBe(""));
    expect(field(/^Authors/).value).toBe("");
  });

  it("keeps the form, and says why, when the node refuses", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, "title is too long"));
    render(MakeWish, { onUnauthorised: vi.fn() });

    await fill("Title", "Krakatit");
    await fireEvent.click(button());

    await screen.findByText("title is too long");
    expect(field("Title").value).toBe("Krakatit");
  });
});

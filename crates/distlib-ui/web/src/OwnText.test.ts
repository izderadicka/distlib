import { fireEvent, render, screen } from "@testing-library/svelte";
import { describe, expect, it, vi } from "vitest";

import OwnText from "./OwnText.svelte";
import { RpcError, Unauthorised } from "./lib/rpc";

function open(mine: string | null, save = vi.fn().mockResolvedValue({})) {
  const onUnauthorised = vi.fn();
  const form = render(OwnText, { noun: "comment", mine, save, onUnauthorised });
  return { form, save, onUnauthorised };
}

const draft = () => screen.getByLabelText("Your comment") as HTMLTextAreaElement;
const button = (name: string) => screen.getByRole("button", { name }) as HTMLButtonElement;

describe("this member's own text", () => {
  it("starts from what this member wrote", () => {
    open("Try the library in Brno.");

    expect(draft().value).toBe("Try the library in Brno.");
  });

  it("saves what is typed", async () => {
    const { save } = open(null);

    await fireEvent.input(draft(), { target: { value: "Me too." } });
    await fireEvent.click(button("Save comment"));

    expect(save).toHaveBeenCalledWith("Me too.");
  });

  it("has nothing to save while the form is blank or unchanged", async () => {
    open("Me too.");
    expect(button("Save comment").disabled).toBe(true);

    await fireEvent.input(draft(), { target: { value: "  " } });

    expect(button("Save comment").disabled).toBe(true);
  });

  it("clears this member's text", async () => {
    const { save } = open("Me too.");

    await fireEvent.click(button("Clear comment"));

    expect(save).toHaveBeenCalledWith(null);
  });

  it("offers no clearing without a text of this member's", () => {
    open(null);

    expect(screen.queryByRole("button", { name: "Clear comment" })).toBeNull();
  });

  it("follows this member's text when it changes", async () => {
    const { form } = open(null);
    expect(draft().value).toBe("");

    await form.rerender({ mine: "Me too." });

    expect(draft().value).toBe("Me too.");
  });

  it("says what the node refused", async () => {
    open(null, vi.fn().mockRejectedValue(new RpcError(-32602, "comment is too long")));

    await fireEvent.input(draft(), { target: { value: "Long." } });
    await fireEvent.click(button("Save comment"));

    await screen.findByText("comment is too long");
  });

  it("hands a refused token up", async () => {
    const { onUnauthorised } = open(null, vi.fn().mockRejectedValue(new Unauthorised()));

    await fireEvent.input(draft(), { target: { value: "Me too." } });
    await fireEvent.click(button("Save comment"));

    await vi.waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });
});

import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import EditItem from "./EditItem.svelte";
import { call, type ItemRecord, RpcError, Unauthorised } from "./lib/rpc";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const MLOCI: ItemRecord = {
  item_id: "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0",
  kind: "audiobook",
  title: "Válka s mloky",
  authors: ["Karel Čapek"],
  genres: ["satire"],
  series: { name: "Mloci", index: 1 },
  year: 1936,
  lang: null,
  description: null,
  replicas: null,
  files: {},
  last_modified: 1790521054348270,
  availability: { held: false, providers: 1 },
};

function open(item = MLOCI) {
  const handlers = { onSaved: vi.fn(), onCancel: vi.fn(), onUnauthorised: vi.fn() };
  const form = render(EditItem, { item, ...handlers });
  return { ...handlers, form };
}

const field = (label: string) => screen.getByLabelText(new RegExp(`^${label}`)) as HTMLInputElement;
const save = () => fireEvent.click(screen.getByRole("button", { name: "Save" }));

describe("editing an item", () => {
  beforeEach(() => {
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ item_id: MLOCI.item_id });
  });

  it("starts from what the item says", () => {
    open();

    expect(field("Title").value).toBe("Válka s mloky");
    expect(field("Authors").value).toBe("Karel Čapek");
    expect(field("Type").value).toBe("audiobook");
    expect(field("Series").value).toBe("Mloci");
    expect(field("Year").value).toBe("1936");
  });

  it("sends only what was changed, and says when it is saved", async () => {
    const { onSaved } = open();
    await fireEvent.input(field("Title"), { target: { value: "War with the Newts" } });
    await fireEvent.input(field("Language"), { target: { value: "en" } });

    await save();

    expect(call).toHaveBeenCalledWith("library.edit_metadata", {
      item_id: MLOCI.item_id,
      fields: { title: "War with the Newts", lang: "en" },
    });
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
  });

  it("sends nothing when nothing was changed", async () => {
    const { onSaved } = open();

    await save();

    await screen.findByText("Nothing has been changed.");
    expect(call).not.toHaveBeenCalled();
    expect(onSaved).not.toHaveBeenCalled();
  });

  it("sends nothing when a field was emptied, and says which it cannot empty", async () => {
    open();
    await fireEvent.input(field("Title"), { target: { value: "" } });
    await fireEvent.input(field("Authors"), { target: { value: " " } });
    await fireEvent.input(field("Language"), { target: { value: "cs" } });

    await save();

    await screen.findByText(/The title, authors cannot be emptied/);
    expect(call).not.toHaveBeenCalled();
  });

  it("says why the node refused the edit, and keeps what was typed", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32000, "no such item: 5e70…"));
    const { onSaved } = open();
    await fireEvent.input(field("Title"), { target: { value: "War with the Newts" } });

    await save();

    await screen.findByText("no such item: 5e70…");
    expect(onSaved).not.toHaveBeenCalled();
    expect(field("Title").value).toBe("War with the Newts");
  });

  it("gives up without sending anything when cancelled", async () => {
    const { onCancel } = open();
    await fireEvent.input(field("Title"), { target: { value: "War with the Newts" } });

    await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));

    expect(onCancel).toHaveBeenCalled();
    expect(call).not.toHaveBeenCalled();
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();
    await fireEvent.input(field("Title"), { target: { value: "War with the Newts" } });

    await save();

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });
});

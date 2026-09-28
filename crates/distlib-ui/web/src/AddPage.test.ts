import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import AddPage from "./AddPage.svelte";
import { router } from "./lib/router.svelte";
import { call, RpcError, Unauthorised } from "./lib/rpc";
import { type Uploaded, UploadFailed, upload } from "./lib/upload";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));
vi.mock("./lib/upload", async (original) => ({
  ...(await original<typeof import("./lib/upload")>()),
  upload: vi.fn(),
}));

const ITEM = "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0";

const file = (name: string, size = 10) => new File(["x".repeat(size)], name);

function open() {
  const onUnauthorised = vi.fn();
  render(AddPage, { onUnauthorised });
  return { onUnauthorised };
}

async function choose(...files: File[]) {
  const input = document.querySelector<HTMLInputElement>('input[type="file"]');
  if (!input) {
    throw new Error("no file input");
  }
  Object.defineProperty(input, "files", { value: files, configurable: true });
  await fireEvent.change(input);
}

const field = (label: string) => screen.getByLabelText(new RegExp(`^${label}`)) as HTMLInputElement;
const submit = () => fireEvent.click(screen.getByRole("button", { name: "Add" }));

describe("the add page", () => {
  beforeEach(() => {
    vi.mocked(upload).mockReset();
    vi.mocked(upload).mockImplementation(async (chosen: File) => ({
      upload: chosen.name.padEnd(32, "0"),
      filename: chosen.name,
      size: chosen.size,
    }));
    vi.mocked(call).mockReset();
    vi.mocked(call).mockResolvedValue({ item_id: ITEM, created: true, title: "Válka s mloky", contributed_files: [] });
  });

  it("uploads the files, adds them as one item with what was said of it, and opens it", async () => {
    open();
    await choose(file("c1.mp3"), file("c2.mp3"));
    await fireEvent.change(field("Type"), { target: { value: "audiobook" } });
    await fireEvent.input(field("Title"), { target: { value: " Válka s mloky " } });
    await fireEvent.input(field("Authors"), { target: { value: "Karel Čapek\n" } });

    await submit();

    await waitFor(() => expect(router.route).toEqual({ page: "item", id: ITEM }));
    expect(vi.mocked(upload).mock.calls.map(([chosen]) => chosen.name)).toEqual(["c1.mp3", "c2.mp3"]);
    expect(call).toHaveBeenCalledWith("library.add", {
      uploads: ["c1.mp3".padEnd(32, "0"), "c2.mp3".padEnd(32, "0")],
      kind: "audiobook",
      title: "Válka s mloky",
      authors: ["Karel Čapek"],
      genres: undefined,
      series: undefined,
      year: undefined,
      lang: undefined,
      description: undefined,
    });
  });

  it("uploads one file at a time, and says how far it has got across them all", async () => {
    let sent!: (bytes: number) => void;
    let finish!: (uploaded: Uploaded) => void;
    vi.mocked(upload).mockImplementation(
      (chosen, onProgress) =>
        new Promise((done) => {
          sent = onProgress;
          finish = done;
          void chosen;
        }),
    );
    open();
    await choose(file("c1.mp3", 1000), file("c2.mp3", 3000));
    await submit();

    sent(400);
    await screen.findByText(/Uploading 1 of 2: c1.mp3, 400 B of\s+4 KB/);
    expect(upload).toHaveBeenCalledOnce();

    finish({ upload: "a".repeat(32), filename: "c1.mp3", size: 1000 });
    await waitFor(() => expect(upload).toHaveBeenCalledTimes(2));
    sent(500);
    await screen.findByText(/Uploading 2 of 2: c2.mp3, 1.5 KB of\s+4 KB/);
    expect(document.querySelector("progress")?.value).toBe(1500);
    expect(document.querySelector("progress")?.max).toBe(4000);
  });

  it("uploads nothing until there are files to add", async () => {
    open();

    await submit();

    await screen.findByText("Choose the item's files first.");
    expect(upload).not.toHaveBeenCalled();
  });

  it("uploads nothing when two files share a name", async () => {
    open();
    await choose(file("cover.jpg"), file("chapter.mp3"), file("chapter.mp3", 20));

    await submit();

    await screen.findByText(/Two of the files are called chapter.mp3/);
    expect(upload).not.toHaveBeenCalled();
  });

  it("uploads nothing when a file has no extension to tell its format by", async () => {
    for (const name of ["README", ".epub", "book."]) {
      open();
      await choose(file("fine.epub"), file(name));

      await submit();

      await screen.findByText(new RegExp(`^${name.replace(".", "\\.")} has no extension`));
      document.body.innerHTML = "";
    }
    expect(upload).not.toHaveBeenCalled();
  });

  it("says when the files are already an item, and that nothing was applied", async () => {
    vi.mocked(call).mockResolvedValue({ item_id: ITEM, created: false, title: "R.U.R.", contributed_files: [] });
    open();
    await choose(file("rur.epub"));
    await fireEvent.input(field("Title"), { target: { value: "A better title" } });

    await submit();

    await screen.findByText(/already in the library/);
    expect(screen.getByText(/the details above were not applied/)).toBeTruthy();
    expect(screen.getByRole("link", { name: "R.U.R." }).getAttribute("href")).toBe(`/items/${ITEM}`);
    expect(router.route).toEqual({ page: "library", query: "", number: 1 });
  });

  it("shows why an upload failed, and adds nothing", async () => {
    vi.mocked(upload).mockRejectedValue(new UploadFailed("larger than the 4 bytes this node takes"));
    open();
    await choose(file("big.mkv"));

    await submit();

    await screen.findByText("larger than the 4 bytes this node takes");
    expect(call).not.toHaveBeenCalled();
    expect((screen.getByRole("button", { name: "Add" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("shows why the node would not add the item", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32602, "no such upload: abc"));
    open();
    await choose(file("rur.epub"));

    await submit();

    await screen.findByText("no such upload: abc");
  });

  it("can be used again while nothing is running, and not while something is", async () => {
    let finish!: (uploaded: Uploaded) => void;
    vi.mocked(upload).mockImplementation(() => new Promise((done) => (finish = done)));
    open();
    await choose(file("rur.epub"));

    await submit();
    await screen.findByText(/Uploading 1 of 1/);
    expect((screen.getByRole("button", { name: "Add" }) as HTMLButtonElement).disabled).toBe(true);
    expect(field("Title").closest("fieldset")?.disabled).toBe(true);

    finish({ upload: "a".repeat(32), filename: "rur.epub", size: 10 });
    await waitFor(() => expect(call).toHaveBeenCalled());
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(upload).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();
    await choose(file("rur.epub"));

    await submit();

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });
});

import { fireEvent, render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Download from "./Download.svelte";
import type { NodeEvent } from "./lib/events";
import { call, RpcError, type TaskState, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ITEM = "5e70b15a0c92500ec3c5ac1c02014271939be7721f4159d016b98ab63a0a81f0";
const OTHER = "f".repeat(64);

/** What the node says of task `task_id` when asked. */
let tasks: Record<number, TaskState>;
/** The id the node gives the next download. */
let next: number;

function finished(task_id: number): TaskState {
  return {
    task_id,
    item_id: ITEM,
    title: "Válka s mloky",
    bytes_done: 23,
    bytes_total: 23,
    files_done: 2,
    files_total: 2,
    state: "finished",
    files: [
      { file: "a".repeat(64), filename: "c1.mp3", path: "/data/downloads/c1.mp3", from: "network" },
      { file: "b".repeat(64), filename: "c2.mp3", path: "/data/downloads/c2.mp3", from: "store" },
    ],
  };
}

function progress(task_id: number, bytes_done: number, files_done: number, item_id = ITEM): NodeEvent {
  return { type: "download.progress", task_id, item_id, bytes_done, bytes_total: 23, files_done, files_total: 2 };
}

const ending = (type: "download.finished" | "download.failed", task_id: number, item_id = ITEM): NodeEvent => ({
  type,
  task_id,
  item_id,
});

function open() {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(Download, { id: ITEM, listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
}

const button = () => screen.getByRole("button") as HTMLButtonElement;
const bar = () => document.querySelector("progress");
const asked = (method: string) => vi.mocked(call).mock.calls.filter(([name]) => name === method);

describe("downloading an item", () => {
  beforeEach(() => {
    tasks = {};
    next = 7;
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string, params: { task_id: number }) => {
      if (method === "library.download") {
        return { task_id: next };
      }
      return tasks[params.task_id];
    }) as typeof call);
  });

  it("asks the node to download the item into its own directory", async () => {
    open();

    await fireEvent.click(button());

    expect(call).toHaveBeenCalledWith("library.download", { item_id: ITEM });
    await waitFor(() => expect(button().textContent?.trim()).toBe("Downloading…"));
    expect(button().disabled).toBe(true);
  });

  it("shows how far it has got, in files and in bytes", async () => {
    const { events } = open();
    await fireEvent.click(button());

    events.tell(progress(7, 11, 1));

    await screen.findByText(/1 of 2 files, 11 B of\s+23 B/);
    expect(bar()?.value).toBe(11);
    expect(bar()?.max).toBe(23);
  });

  it("never moves the bar backwards, though the node's count may", async () => {
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 15, 1));
    await screen.findByText(/15 B/);

    events.tell(progress(7, 4, 1));
    events.tell(progress(7, 9, 2));

    await screen.findByText(/2 of 2 files, 15 B of/);
    expect(bar()?.value).toBe(15);
  });

  it("says where the files went, and whether each was fetched, when it finishes", async () => {
    tasks[7] = finished(7);
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 11, 1));

    events.tell(ending("download.finished", 7));

    await screen.findByText("/data/downloads/c1.mp3");
    expect(screen.getByText("/data/downloads/c2.mp3")).toBeTruthy();
    expect(screen.getByText("fetched")).toBeTruthy();
    expect(screen.getByText("had it")).toBeTruthy();
    expect(bar()).toBeNull();
    expect(button().disabled).toBe(false);
  });

  it("says why it failed, and can be asked again", async () => {
    tasks[7] = { ...finished(7), state: "failed", error: "nobody could provide c2.mp3" } as TaskState;
    const { events } = open();
    await fireEvent.click(button());

    events.tell(ending("download.failed", 7));

    await screen.findByText("The download failed: nobody could provide c2.mp3");
    expect(button().disabled).toBe(false);
    expect(button().textContent?.trim()).toBe("Download");
  });

  it("shows a download the node refused, and can be asked again", async () => {
    vi.mocked(call).mockRejectedValueOnce(
      new RpcError(-32000, "/data/downloads/c1.mp3 already exists and is not this file"),
    );
    open();

    await fireEvent.click(button());

    await screen.findByText("/data/downloads/c1.mp3 already exists and is not this file");
    expect(button().disabled).toBe(false);
  });

  it("picks up a download already running, as a reloaded page hears it", async () => {
    const { events } = open();

    events.tell(progress(3, 11, 1));

    await screen.findByText(/1 of 2 files/);
    expect(button().disabled).toBe(true);
    expect(asked("library.download")).toHaveLength(0);
  });

  it("follows its download when the stream names it before the node has answered", async () => {
    let answer!: (started: { task_id: number }) => void;
    vi.mocked(call).mockImplementationOnce(
      (() => new Promise((done) => (answer = done))) as unknown as typeof call,
    );
    const { events } = open();
    await fireEvent.click(button());

    events.tell(progress(7, 15, 1));
    await screen.findByText(/15 B/);
    answer({ task_id: 7 });
    await waitFor(() => expect(button().textContent?.trim()).toBe("Downloading…"));

    // Nothing started over: what was heard stands.
    expect(bar()?.value).toBe(15);
  });

  it("keeps to the download it follows while it runs", async () => {
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 11, 1));
    await screen.findByText(/11 B/);

    events.tell(progress(8, 20, 2));
    events.tell(ending("download.finished", 8));

    await new Promise((done) => setTimeout(done, 0));
    expect(bar()?.value).toBe(11);
    expect(asked("library.task")).toHaveLength(0);
  });

  it("follows a new download once the last has ended", async () => {
    tasks[7] = finished(7);
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 20, 1));
    events.tell(ending("download.finished", 7));
    await screen.findByText("/data/downloads/c1.mp3");

    next = 8;
    await fireEvent.click(button());
    events.tell(progress(8, 3, 0));

    // From its own start, not the last one's end.
    await screen.findByText(/0 of 2 files, 3 B/);
    expect(bar()?.value).toBe(3);
    expect(screen.queryByText("/data/downloads/c1.mp3")).toBeNull();
  });

  it("pays no attention to downloads of other items", async () => {
    const { events } = open();

    events.tell(progress(3, 11, 1, OTHER));
    events.tell(ending("download.finished", 3, OTHER));

    await new Promise((done) => setTimeout(done, 0));
    expect(bar()).toBeNull();
    expect(button().disabled).toBe(false);
    expect(call).not.toHaveBeenCalled();
  });

  it("asks after a running download on a resync, in case its ending was missed", async () => {
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 11, 1));
    await screen.findByText(/11 B/);
    tasks[7] = finished(7);

    events.tell({ type: "resync" });

    await screen.findByText("/data/downloads/c1.mp3");
    expect(call).toHaveBeenLastCalledWith("library.task", { task_id: 7 });
  });

  it("keeps showing a download the resync finds still running", async () => {
    const { events } = open();
    await fireEvent.click(button());
    events.tell(progress(7, 11, 1));
    await screen.findByText(/11 B/);
    tasks[7] = { ...finished(7), state: "running" } as TaskState;

    events.tell({ type: "resync" });

    await waitFor(() => expect(asked("library.task")).toHaveLength(1));
    expect(bar()?.value).toBe(11);
  });

  it("asks after nothing on a resync when nothing is running", async () => {
    const { events } = open();

    events.tell({ type: "resync" });

    expect(call).not.toHaveBeenCalled();
  });

  it("asks after nothing on a resync once its download has ended", async () => {
    tasks[7] = finished(7);
    const { events } = open();
    await fireEvent.click(button());
    events.tell(ending("download.finished", 7));
    await screen.findByText("/data/downloads/c1.mp3");

    events.tell({ type: "resync" });

    expect(asked("library.task")).toHaveLength(1);
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();

    await fireEvent.click(button());

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });

  it("stops listening when it is closed", () => {
    const { events, page } = open();
    expect(events.listening()).toBe(1);

    page.unmount();

    expect(events.listening()).toBe(0);
  });
});

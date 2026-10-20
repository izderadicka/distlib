import { render, screen, waitFor } from "@testing-library/svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NodePage from "./NodePage.svelte";
import { call, type GroupSettings, type Members, type NodeStatus, RpcError, Unauthorised } from "./lib/rpc";
import { fakeListen } from "./testing";

vi.mock("./lib/rpc", async (original) => ({
  ...(await original<typeof import("./lib/rpc")>()),
  call: vi.fn(),
}));

const ME = "a".repeat(64);
const CAROL = "c".repeat(64);
const DAVE = "d".repeat(64);

/** Not the defaults, so each line shows the value it was given. */
const SETTINGS: GroupSettings = {
  replicas: 2,
  cap_bytes: 1_000_000_000_000,
  weight_step_bytes: 5_000_000_000,
  min_pledge_bytes: 50_000_000_000,
  pending_expiry: 64,
  settle_secs: 600,
  max_settle_secs: 7200,
  emergency_after_secs: 172_800,
  min_transfer_rate: 2_000_000,
  probes: true,
  probe_every_secs: 90,
  failure_window_secs: 86_400,
};

const STATUS: NodeStatus = {
  member: ME,
  group: "b".repeat(64),
  core: true,
  members: 2,
  core_group: [ME],
  changed_at: 4,
  raft: "Leader",
  leader: ME,
  followed_upto: null,
  pending: 1,
  settings: SETTINGS,
  storage: {
    limit_bytes: 500_000_000_000,
    own_bytes: 1_500_000_000,
    custody_bytes: 120_000_000_000,
    cache_bytes: 7_000_000,
    mandatory_over_limit: false,
    last_collection_at: null,
  },
  sync: { neighbours: [], last_sync: [] },
};

/** When a sync round finished, as a node answers it, and the moment it is. */
const FINISHED = 1790521054348270;
const FINISHED_AT = "2026-09-27T14:57:34.348Z";

function members(...extra: Members["members"]): Members {
  return {
    group: STATUS.group,
    changed_at: 4,
    members: [
      // A founder names nobody, themselves included.
      { member: ME, name: "", pledge_bytes: 1_500_000_000, core: true },
      { member: CAROL, name: "carol", pledge_bytes: 0, core: false },
      ...extra,
    ],
  };
}

/** What the node answers with, while the test lets it. */
let answer: { status: NodeStatus; members: Members };

/** Renders the page, and hands back what it hears the node through. */
function open() {
  const events = fakeListen();
  const onUnauthorised = vi.fn();
  const page = render(NodePage, { listen: events.listen, onUnauthorised });
  return { events, onUnauthorised, page };
}

/** The members table's rows, header left out, as their cells' text. */
const cells = () =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => [...row.querySelectorAll("td")].map((cell) => cell.textContent?.trim()));

/**
 * Matches a `tag` whose whole text, however many pieces it was rendered in,
 * is `text`.
 */
function wholly(tag: string, text: string) {
  return (_: string, element: Element | null) =>
    element?.tagName.toLowerCase() === tag &&
    element.textContent?.replace(/\s+/g, " ").trim() === text;
}

/** What the section headed `heading` says, term by term. */
function described(heading: string): Record<string, string | null | undefined> {
  const section = screen.getByRole("heading", { name: heading }).closest("section");
  return Object.fromEntries(
    [...(section?.querySelectorAll("dt") ?? [])].map((term) => [
      term.textContent,
      term.nextElementSibling?.textContent,
    ]),
  );
}

describe("the node page", () => {
  beforeEach(() => {
    answer = { status: STATUS, members: members() };
    vi.mocked(call).mockReset();
    vi.mocked(call).mockImplementation((async (method: string) =>
      method === "node.status" ? answer.status : answer.members) as typeof call);
  });

  it("loads when it opens, and shows the node and its members", async () => {
    open();

    await screen.findByText(ME);
    expect(screen.getByText(STATUS.group ?? "")).toBeTruthy();
    expect(screen.getByText(wholly("dd", "core — leader"))).toBeTruthy();
    expect(screen.getByText("1 proposal awaiting approval")).toBeTruthy();

    const rows = screen.getAllByRole("row").slice(1).map((row) => row.textContent);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toContain("no name");
    expect(rows[0]).toContain("core (leader)");
    expect(rows[0]).toContain("1.5 GB");
    expect(rows[1]).toContain("carol");
    expect(rows[1]).toContain("follower");
    expect(rows[1]).toContain("0 B");
  });

  it("shows the settings the group was founded with", async () => {
    open();

    await screen.findByText("Group settings");
    const said = Object.fromEntries(
      [...document.querySelectorAll("section:nth-of-type(2) dt")].map((term) => [
        term.textContent,
        term.nextElementSibling?.textContent,
      ]),
    );
    expect(said).toEqual({
      "Copies of each item": "2, the adder's own included",
      "Weight cap": "1 TB",
      "Weight step": "5 GB",
      "Minimum pledge": "50 GB",
      "Custody follows membership": "after 10 minutes without a change, 2 hours at most",
      "Emergency copy": "after 2 days with one copy left",
      "Minimum transfer rate": "2 MB/s",
      Probes: "every 90 seconds",
      "Probe failures count for": "1 day",
      "Proposals wait": "64 changes for approval",
    });
  });

  it("says when the group does not probe", async () => {
    answer = { ...answer, status: { ...STATUS, settings: { ...SETTINGS, probes: false } } };
    open();

    await screen.findByText("Group settings");
    expect(screen.getByText(wholly("dd", "off"))).toBeTruthy();
  });

  it("shows no settings before there is a group", async () => {
    answer = { ...answer, status: { ...STATUS, group: null, settings: null } };
    open();

    await screen.findByText(ME);
    expect(screen.queryByText("Group settings")).toBeNull();
  });

  it("shows each member's place in the catalogue's swarm", async () => {
    answer = {
      status: {
        ...STATUS,
        sync: {
          neighbours: [CAROL],
          last_sync: [
            { member: CAROL, finished: FINISHED, ok: true },
            { member: DAVE, finished: FINISHED, ok: false },
          ],
        },
      },
      members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }),
    };
    open();

    await screen.findByText("dave");
    const [me, carol, dave] = cells();
    expect(me.slice(-2)).toEqual(["—", "—"]);
    expect(carol[4]).toBe("yes");
    expect(carol[5]).not.toContain("failed");
    expect(dave[4]).toBe("—");
    expect(dave[5]).toMatch(/ failed$/);
    const times = [...document.querySelectorAll("td time")].map((time) => time.getAttribute("datetime"));
    expect(times).toEqual([FINISHED_AT, FINISHED_AT]);
  });

  it("shows what this node keeps, against its storage limit", async () => {
    open();

    await screen.findByText("Storage");
    expect(described("Storage")).toEqual({
      Limit: "500 GB",
      "Added here": "1.5 GB",
      "Kept for the group": "120 GB",
      Cache: "7 MB",
    });
    expect(screen.getByText(/the cache goes first/)).toBeTruthy();
    expect(screen.queryByText(/over the limit on their own/)).toBeNull();
  });

  it("says when there is no storage limit", async () => {
    answer = { ...answer, status: { ...STATUS, storage: { ...STATUS.storage, limit_bytes: null } } };
    open();

    await screen.findByText("Storage");
    expect(described("Storage").Limit).toBe("none");
    expect(screen.queryByText(/the cache goes first/)).toBeNull();
  });

  it("warns when what is added and kept for the group is over the limit alone", async () => {
    answer = {
      ...answer,
      status: { ...STATUS, storage: { ...STATUS.storage, mandatory_over_limit: true } },
    };
    open();

    await screen.findByText(/over the limit on their own/);
    expect(screen.queryByText(/the cache goes first/)).toBeNull();
  });

  it.each([
    { type: "catalogue.item_added", item_id: "x" },
    { type: "download.finished", task_id: 1, item_id: "x" },
  ])("loads again on $type, which changes what is kept", async (event) => {
    const { events } = open();
    await screen.findByText("Storage");
    expect(described("Storage").Cache).toBe("7 MB");

    answer = { ...answer, status: { ...STATUS, storage: { ...STATUS.storage, cache_bytes: 9_000_000 } } };
    events.tell(event);

    await waitFor(() => expect(described("Storage").Cache).toBe("9 MB"));
  });

  it("loads again when the catalogue's swarm changes", async () => {
    const { events } = open();
    await screen.findByText("carol");
    expect(cells()[1][4]).toBe("—");

    answer = { ...answer, status: { ...STATUS, sync: { neighbours: [CAROL], last_sync: [] } } };
    events.tell({ type: "sync.status" });

    await waitFor(() => expect(cells()[1][4]).toBe("yes"));
  });

  it("loads again when the membership changes", async () => {
    const { events } = open();
    await screen.findByText("carol");

    answer = { status: STATUS, members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }) };
    events.tell({ type: "membership.changed" });

    await screen.findByText("dave");
  });

  it("loads again on a resync: whatever was missed is covered by it", async () => {
    const { events } = open();
    await screen.findByText("carol");

    answer = { status: STATUS, members: members({ member: DAVE, name: "dave", pledge_bytes: 0, core: false }) };
    events.tell({ type: "resync" });

    await screen.findByText("dave");
  });

  it("does not load again for news that changes nothing it shows", async () => {
    const { events } = open();
    await screen.findByText("carol");
    const asked = vi.mocked(call).mock.calls.length;

    events.tell({ type: "bookmark.changed", item_id: "x" });

    expect(call).toHaveBeenCalledTimes(asked);
  });

  it("hands a refused token up to whoever signs the tab in", async () => {
    vi.mocked(call).mockRejectedValue(new Unauthorised());
    const { onUnauthorised } = open();

    await waitFor(() => expect(onUnauthorised).toHaveBeenCalled());
  });

  it("shows any other failure", async () => {
    vi.mocked(call).mockRejectedValue(new RpcError(-32000, "the read model is not ready"));
    open();

    await screen.findByText("the read model is not ready");
  });

  it("stops listening when it is closed", () => {
    const { events, page } = open();
    expect(events.listening()).toBe(1);

    page.unmount();

    expect(events.listening()).toBe(0);
  });
});

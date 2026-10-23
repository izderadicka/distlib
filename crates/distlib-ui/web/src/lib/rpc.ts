// A typed client for the node's JSON-RPC API (`POST /rpc`).
//
// Every method the page calls is listed in `Methods` with its params and its
// result, so a call with the wrong params, or a result read as the wrong
// shape, is a type error here rather than an `undefined` in the page. Only
// what a page calls is listed; the types follow `distlib-api`'s answers.

import type { Fields } from "./metadata";
import { forgetToken, token } from "./token";

/** `node.status`. */
export interface NodeStatus {
  member: string;
  group: string | null;
  core: boolean;
  members: number;
  core_group: string[];
  changed_at: number;
  /** The Raft role, on a voter; `null` on a follower. */
  raft: string | null;
  leader: string | null;
  /** How far a follower has read the log; `null` on a voter. */
  followed_upto: number | null;
  pending: number;
  /** What the group decided when it was founded; `null` before there is one. */
  settings: GroupSettings | null;
  /** This node's custody and the group's load; `null` until custody is computed. */
  custody: Custody | null;
  /** What this node keeps, against its storage limit. */
  storage: Storage;
  /** The catalogue's swarm, as this node sees it. */
  sync: {
    /** The members this node is a catalogue gossip neighbour of. */
    neighbours: string[];
    last_sync: LastSync[];
  };
}

/** What a node is custodian of, and how loaded the group's pledges are. */
export interface Custody {
  items: number;
  bytes: number;
  /** Each item's size per custodian slot, over the group's total weight: above 1, some items are a copy short. `null` when no pledge weighs anything. */
  load_factor: number | null;
}

/** The bytes a node keeps, each blob once under the strongest reason. */
export interface Storage {
  /** `[storage] limit_bytes`; `null` for no limit. */
  limit_bytes: number | null;
  /** What this node added. */
  own_bytes: number;
  /** What it keeps for the group's custody. */
  custody_bytes: number;
  /** What it downloaded or kept after custody, evicted when space is short. */
  cache_bytes: number;
  /** Own and custody alone over the limit: nothing left to evict. */
  mandatory_over_limit: boolean;
  /** When the last garbage collection pass began, in microseconds since the epoch. */
  last_collection_at: number | null;
}

/** A group's settings, fixed when it was founded. */
export interface GroupSettings {
  /** Copies kept of each item, the adder's own included. */
  replicas: number;
  cap_bytes: number;
  weight_step_bytes: number;
  min_pledge_bytes: number;
  /** Changes a proposal waits for its approvals. */
  pending_expiry: number;
  settle_secs: number;
  max_settle_secs: number;
  emergency_after_secs: number;
  /** Bytes per second. */
  min_transfer_rate: number;
  probes: boolean;
  probe_every_secs: number;
  failure_window_secs: number;
}

/** How the last catalogue sync round with one member ended. */
export interface LastSync {
  member: string;
  /** Microseconds since the epoch. */
  finished: number;
  ok: boolean;
}

/** One entry of `group.members`. */
export interface Member {
  member: string;
  name: string;
  pledge_bytes: number;
  /** What it added requires it to pledge, and never less than the group's minimum (D8). */
  required_pledge_bytes: number;
  core: boolean;
}

/** `group.members`. */
export interface Members {
  group: string | null;
  changed_at: number;
  members: Member[];
}

/** Where an item sits in a series. */
export interface Series {
  name: string;
  /** Left out, rather than `null`, when nobody said. */
  index?: number;
}

/**
 * What `library.list` and `library.search` show of one item. A field nobody
 * has set is `null`.
 */
export interface ItemSummary {
  item_id: string;
  kind: "ebook" | "audiobook" | "video" | "other" | null;
  title: string | null;
  authors: string[] | null;
  genres: string[] | null;
  series: Series | null;
  year: number | null;
}

/** Who has an item: this node, and how many online members. */
export interface Availability {
  held: boolean;
  /** `null` while some online member's holdings are not known yet. */
  providers: number | null;
}

/** What `library.item` says of who has an item: also who is online, this node left out. */
export interface ItemAvailability extends Availability {
  online: string[];
}

/** One result of `library.list` or `library.search`. */
export type Hit = ItemSummary & Availability;

/** One file of an item. What nobody said is left out, rather than `null`. */
export interface FileRecord {
  role: "content" | "cover" | "subtitle" | "metadata" | "other";
  format: string;
  size: number;
  filename: string;
  seq?: number;
  disc?: number;
  title?: string;
  /** Seconds. */
  duration?: number;
}

/** Current members' ratings of one item. */
export interface Ratings {
  /** `null` while nobody has rated it. */
  average: number | null;
  count: number;
  /** This node's member's own, one to five. */
  mine: number | null;
}

/** One member's review of an item. */
export interface ReviewRecord {
  member: string;
  review: string;
}

/** One member's bookmark on an item. */
export interface BookmarkRecord {
  member: string;
  bookmark_id: string;
  position: string;
  note: string;
  /** Microseconds since the epoch, both. */
  created_at: number;
  last_modified: number;
}

/** `community.bookmarks`: a bookmark with what it points into, and who left it. */
export interface ListedBookmark extends BookmarkRecord {
  item_id: string;
  /** The item's title, `null` while it has none. */
  title: string | null;
  /** The member's name, `null` while the group does not know one. */
  member_name: string | null;
}

/** `library.item`: everything the read model holds for one item. */
export interface ItemRecord extends ItemSummary {
  lang: string | null;
  description: string | null;
  /** By each file's content hash. */
  files: Record<string, FileRecord>;
  /** Microseconds since the epoch. */
  last_modified: number;
  /** Whoever wrote one of its files, members or not any more. */
  added_by: string[];
  /** Who keeps it for the group besides its adders; `null` until custody is computed. */
  custodians: string[] | null;
  availability: ItemAvailability;
  ratings: Ratings;
  reviews: ReviewRecord[];
  bookmarks: BookmarkRecord[];
}

/** One wish, as its members' entries resolve it (D10). */
export interface WishRecord {
  wish_id: string;
  /** Whoever wished for it first. */
  creator: string;
  kind: ItemSummary["kind"];
  title: string;
  authors: string[] | null;
  description: string | null;
  /** Microseconds since the epoch. */
  created_at: number | null;
  /** `fulfilled` once any member says an item answers it. */
  status: "open" | "fulfilled";
  /** Every member's answer: the item, and its title once this node has it. */
  fulfilled_by: { member: string; item_id: string; title: string | null }[];
  comments: { member: string; comment: string }[];
}

/** How far a download has got, in bytes and in files written. */
export interface Progress {
  bytes_done: number;
  bytes_total: number;
  files_done: number;
  files_total: number;
}

/** One file a finished download wrote, and where its bytes came from. */
export interface Downloaded {
  file: string;
  filename: string;
  path: string;
  from: "network" | "store" | "destination";
}

/** `library.task`: a download, and where it has got to. */
export type TaskState = {
  task_id: number;
  item_id: string;
  title: string | null;
} & Progress &
  (
    | { state: "running" }
    | { state: "finished"; files: Downloaded[] }
    | { state: "failed"; error: string }
  );

/** A pledge short of what a member's adds require (D8). */
export interface UnderPledged {
  pledge_bytes: number;
  required_pledge_bytes: number;
}

/** A page of items, and how many there are in all. */
export interface ItemPage {
  results: Hit[];
  total: number;
}

interface Methods {
  "node.status": { params: null; result: NodeStatus };
  "group.members": { params: null; result: Members };
  "library.list": { params: { offset: number; limit: number }; result: ItemPage };
  "library.search": {
    params: { query: string; offset: number; limit: number };
    result: ItemPage;
  };
  "library.item": { params: { item_id: string }; result: ItemRecord };
  "library.add": {
    params: { uploads: string[] } & Fields;
    result: {
      item_id: string;
      created: boolean;
      title: string | null;
      contributed_files: string[];
      /** This node's pledge and what its adds now require, when the pledge falls short (D8). */
      under_pledged: UnderPledged | null;
    };
  };
  /** Writes the fields given, and only those. */
  "library.edit_metadata": { params: { item_id: string; fields: Fields }; result: { item_id: string } };
  /** Without `dest`: into the node's own `[library] download_dir`. */
  "library.download": { params: { item_id: string }; result: { task_id: number } };
  "library.task": { params: { task_id: number }; result: TaskState };
  /** `rating: null` clears this member's rating. */
  "community.rate": { params: { item_id: string; rating: number | null }; result: { item_id: string } };
  /** `review: null` clears this member's review. */
  "community.review": { params: { item_id: string; review: string | null }; result: { item_id: string } };
  /** With `bookmark_id`: edits that bookmark of this member's; without, leaves a new one. */
  "community.bookmark": {
    params: { item_id: string; bookmark_id?: string; position: string; note: string };
    result: { item_id: string; bookmark_id: string };
  };
  "community.bookmark_delete": {
    params: { item_id: string; bookmark_id: string };
    result: { item_id: string; bookmark_id: string };
  };
  /** Every member's bookmarks, newest first; `q` searches the note, the item's title and the member's name. */
  "community.bookmarks": {
    params: { q?: string; member?: string; item_id?: string };
    result: { bookmarks: ListedBookmark[] };
  };
  /** Every wish, newest first. */
  "community.wish_list": { params: Record<string, never>; result: { wishes: WishRecord[] } };
  "community.wish_create": {
    params: { title: string; kind?: NonNullable<ItemSummary["kind"]>; authors?: string[]; description?: string };
    result: { wish_id: string };
  };
  /** `comment: null` clears this member's comment. */
  "community.wish_comment": { params: { wish_id: string; comment: string | null }; result: { wish_id: string } };
  "community.wish_fulfill": {
    params: { wish_id: string; item_id: string };
    result: { wish_id: string; item_id: string };
  };
}

/** The node answered, and the answer was an error. */
export class RpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(message);
  }
}

/** The node refused this tab's token, or the tab has none. */
export class Unauthorised extends Error {
  constructor() {
    super("the node refused this tab's token");
  }
}

/** Calls `method` on the node. */
export async function call<M extends keyof Methods>(
  method: M,
  params: Methods[M]["params"],
): Promise<Methods[M]["result"]> {
  const held = token();
  if (held === null) {
    throw new Unauthorised();
  }
  const response = await fetch("/rpc", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${held}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  if (response.status === 401) {
    forgetToken();
    throw new Unauthorised();
  }
  // Every answer is JSON, refusals included (D9), so there is one parser.
  const answer = await response.json();
  if (answer.error) {
    throw new RpcError(answer.error.code, answer.error.message);
  }
  return answer.result;
}

/**
 * Runs `action` and answers what went wrong, in words, or `null` if nothing
 * did. A refused token is handed to `onUnauthorised` rather than shown: the
 * tab has to be signed in again, which is not this page's to say.
 */
export async function failureOf(action: () => Promise<unknown>, onUnauthorised: () => void): Promise<string | null> {
  try {
    await action();
    return null;
  } catch (error) {
    if (error instanceof Unauthorised) {
      onUnauthorised();
      return null;
    }
    return error instanceof Error ? error.message : String(error);
  }
}

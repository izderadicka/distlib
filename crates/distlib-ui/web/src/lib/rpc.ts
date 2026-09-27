// A typed client for the node's JSON-RPC API (`POST /rpc`).
//
// Every method the page calls is listed in `Methods` with its params and its
// result, so a call with the wrong params, or a result read as the wrong
// shape, is a type error here rather than an `undefined` in the page. Only
// what a page calls is listed; the types follow `distlib-api`'s answers.

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
}

/** One entry of `group.members`. */
export interface Member {
  member: string;
  name: string;
  pledge_bytes: number;
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

/** `library.item`: everything the read model holds for one item. */
export interface ItemRecord extends ItemSummary {
  lang: string | null;
  description: string | null;
  replicas: number | null;
  /** By each file's content hash. */
  files: Record<string, FileRecord>;
  /** Microseconds since the epoch. */
  last_modified: number;
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

/** A page of items, and how many there are in all. */
export interface ItemPage {
  results: ItemSummary[];
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
  /** Without `dest`: into the node's own `[library] download_dir`. */
  "library.download": { params: { item_id: string }; result: { task_id: number } };
  "library.task": { params: { task_id: number }; result: TaskState };
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

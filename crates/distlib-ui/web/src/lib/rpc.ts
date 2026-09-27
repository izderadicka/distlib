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

interface Methods {
  "node.status": { params: null; result: NodeStatus };
  "group.members": { params: null; result: Members };
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

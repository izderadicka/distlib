import { beforeEach, describe, expect, it, vi } from "vitest";

import { call, RpcError, Unauthorised } from "./rpc";
import { token } from "./token";

/** A `fetch` that answers every request with `status` and `body`. */
function answering(status: number, body: unknown) {
  const fetch = vi.fn(async () => new Response(JSON.stringify(body), { status }));
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

describe("the RPC client", () => {
  beforeEach(() => {
    sessionStorage.setItem("distlib.token", "secret");
  });

  it("calls a method with the token, and answers with its result", async () => {
    const fetch = answering(200, {
      jsonrpc: "2.0",
      id: 1,
      result: { group: "g", changed_at: 3, members: [] },
    });

    const members = await call("group.members", null);

    expect(members.changed_at).toBe(3);
    expect(fetch).toHaveBeenCalledOnce();
    const [path, init] = fetch.mock.calls[0] as unknown as [string, RequestInit];
    expect(path).toBe("/rpc");
    expect(init.method).toBe("POST");
    expect(new Headers(init.headers).get("authorization")).toBe("Bearer secret");
    expect(JSON.parse(init.body as string)).toEqual({
      jsonrpc: "2.0",
      id: 1,
      method: "group.members",
      params: null,
    });
  });

  it("turns an error answer into an RpcError with its code", async () => {
    answering(200, {
      jsonrpc: "2.0",
      id: 1,
      error: { code: -32000, message: "this node is in no group yet" },
    });

    const failed = call("node.status", null);

    await expect(failed).rejects.toBeInstanceOf(RpcError);
    await expect(failed).rejects.toMatchObject({
      code: -32000,
      message: "this node is in no group yet",
    });
  });

  it("forgets a token the node refuses", async () => {
    answering(401, { jsonrpc: "2.0", id: null, error: { code: -32001, message: "unauthorised" } });

    await expect(call("node.status", null)).rejects.toBeInstanceOf(Unauthorised);
    expect(token()).toBeNull();
  });

  it("does not ask the node at all without a token", async () => {
    sessionStorage.clear();
    const fetch = answering(200, {});

    await expect(call("node.status", null)).rejects.toBeInstanceOf(Unauthorised);
    expect(fetch).not.toHaveBeenCalled();
  });
});

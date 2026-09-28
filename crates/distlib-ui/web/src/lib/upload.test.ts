import { beforeEach, describe, expect, it, vi } from "vitest";

import { Unauthorised } from "./rpc";
import { token } from "./token";
import { UploadFailed, upload } from "./upload";

/** A stand-in for the browser's `XMLHttpRequest`, answered by the test. */
class FakeRequest {
  static last: FakeRequest;
  method = "";
  url = "";
  headers: Record<string, string> = {};
  sent: unknown;
  status = 0;
  responseText = "";
  upload: { onprogress: ((event: { loaded: number }) => void) | null } = { onprogress: null };
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;

  constructor() {
    FakeRequest.last = this;
  }
  open(method: string, url: string) {
    this.method = method;
    this.url = url;
  }
  setRequestHeader(name: string, value: string) {
    this.headers[name] = value;
  }
  send(body: unknown) {
    this.sent = body;
  }
  answer(status: number, body: unknown) {
    this.status = status;
    this.responseText = typeof body === "string" ? body : JSON.stringify(body);
    this.onload?.();
  }
}

const FILE = new File(["a book"], "Válka s mloky.epub");

describe("uploading a file", () => {
  beforeEach(() => {
    vi.stubGlobal("XMLHttpRequest", FakeRequest);
    sessionStorage.setItem("distlib.token", "secret");
  });

  it("sends the file itself, named, with the token", async () => {
    const sending = upload(FILE, () => {});
    const request = FakeRequest.last;

    expect(request.method).toBe("POST");
    expect(request.url).toBe("/upload?filename=V%C3%A1lka%20s%20mloky.epub");
    expect(request.headers.Authorization).toBe("Bearer secret");
    expect(request.sent).toBe(FILE);

    request.answer(200, { upload: "ab".repeat(16), filename: FILE.name, size: 6 });
    await expect(sending).resolves.toEqual({ upload: "ab".repeat(16), filename: FILE.name, size: 6 });
  });

  it("says how much has been sent as it goes", () => {
    const heard: number[] = [];
    void upload(FILE, (sent) => heard.push(sent));

    FakeRequest.last.upload.onprogress?.({ loaded: 2 });
    FakeRequest.last.upload.onprogress?.({ loaded: 6 });

    expect(heard).toEqual([2, 6]);
  });

  it("says why the node refused it", async () => {
    const sending = upload(FILE, () => {});

    FakeRequest.last.answer(413, {
      jsonrpc: "2.0",
      id: null,
      error: { code: -32602, message: "larger than the 4 bytes this node takes" },
    });

    await expect(sending).rejects.toEqual(new UploadFailed("larger than the 4 bytes this node takes"));
  });

  it("says what it can of an answer that is not the node's", async () => {
    const sending = upload(FILE, () => {});

    FakeRequest.last.answer(502, "<html>Bad Gateway</html>");

    await expect(sending).rejects.toEqual(new UploadFailed("the node answered 502"));
  });

  it("says a file may be too large when the connection is cut before an answer", async () => {
    const sending = upload(FILE, () => {});

    FakeRequest.last.onerror?.();

    await expect(sending).rejects.toThrow(/cut off.*larger than the node takes/);
  });

  it("forgets a token the node refuses", async () => {
    const sending = upload(FILE, () => {});

    FakeRequest.last.answer(401, { error: { code: -32001, message: "unauthorised" } });

    await expect(sending).rejects.toBeInstanceOf(Unauthorised);
    expect(token()).toBeNull();
  });

  it("sends nothing from a tab that holds no token", async () => {
    sessionStorage.clear();
    FakeRequest.last = undefined as unknown as FakeRequest;

    await expect(upload(FILE, () => {})).rejects.toBeInstanceOf(Unauthorised);
    expect(FakeRequest.last).toBeUndefined();
  });
});

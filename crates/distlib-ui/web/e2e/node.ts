// A real `distlib` node for a browser to talk to: a group of one, founded in
// a directory of its own, on ports nobody else is using.
//
// The binary is `DISTLIB_BIN`, or the workspace's debug build. A debug build
// reads the UI from `web/dist` as it runs, so `npm run build` before these
// tests is what they test — `npm run e2e` does both.

import { type ChildProcess, execFileSync, spawn } from "node:child_process";
import { createSocket } from "node:dgram";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const BINARY =
  process.env.DISTLIB_BIN ?? resolve(import.meta.dirname, "../../../../target/debug/distlib");

/** How long a node gets to start before the test gives up on it. */
const STARTUP_MS = 60_000;

export class Node {
  /** The page's address. */
  readonly base: string;
  /** The link `distlib ui` prints: the page, signed in. */
  readonly link: string;
  readonly id: string;

  private process: ChildProcess | null;

  private constructor(
    private readonly dir: string,
    ports: { api: number },
    id: string,
    process: ChildProcess,
  ) {
    this.base = `http://127.0.0.1:${ports.api}/`;
    this.id = id;
    this.process = process;
    this.link = distlib(dir, "ui").trim();
  }

  /** Founds a group of one and waits for its API. */
  static async found(): Promise<Node> {
    if (!existsSync(BINARY)) {
      throw new Error(`no distlib binary at ${BINARY}: run \`cargo build -p distlib\`, or set DISTLIB_BIN`);
    }
    const dir = mkdtempSync(join(tmpdir(), "distlib-e2e-"));
    const ports = { transport: await freeUdpPort(), api: await freeTcpPort() };
    const net = `[net]\nbind_addr_v4 = "127.0.0.1:${ports.transport}"\nrelay_mode = "disabled"\n`;
    const api = `[api]\nbind_addr = "127.0.0.1:${ports.api}"\n`;
    writeFileSync(join(dir, "config.toml"), `${net}\n${api}`);
    const id = identity(dir);
    writeFileSync(
      join(dir, "config.toml"),
      `${net}\n${api}\n[consensus]\ncore = [\n  { member = "${id}", addrs = ["127.0.0.1:${ports.transport}"] },\n]\n`,
    );
    try {
      return new Node(dir, ports, id, await run(dir, ["run", "--found-group"]));
    } catch (error) {
      rmSync(dir, { recursive: true, force: true });
      throw error;
    }
  }

  /** Where a download started from the page writes: `[library] download_dir`'s default. */
  get downloads(): string {
    return join(this.dir, "downloads");
  }

  /** Starts the node again after [`stop`]. */
  async start(): Promise<void> {
    this.process = await run(this.dir, ["run"]);
  }

  /** Stops the node the way an operator does, and waits for it to be gone. */
  async stop(): Promise<void> {
    const process = this.process;
    this.process = null;
    if (process === null || process.exitCode !== null) {
      return;
    }
    const gone = new Promise((done) => process.once("exit", done));
    process.kill("SIGINT");
    await gone;
  }

  /**
   * Adds an item of one file, holding `content`, from this node's own CLI,
   * with `details` as `distlib add` takes them (`--kind` and on).
   */
  add(filename: string, content: string, ...details: string[]): void {
    const file = join(this.dir, filename);
    writeFileSync(file, content);
    distlib(this.dir, "add", file, ...details);
  }

  /** Admits `member` from this node's own CLI. */
  admit(member: string, name: string): void {
    distlib(this.dir, "admit", member, "--name", name);
  }

  async dispose(): Promise<void> {
    await this.stop();
    rmSync(this.dir, { recursive: true, force: true });
  }
}

/** A member id nobody has used: a fresh identity in a directory of its own. */
export function aStranger(): string {
  const dir = mkdtempSync(join(tmpdir(), "distlib-e2e-stranger-"));
  try {
    return identity(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function distlib(dir: string, ...args: string[]): string {
  return execFileSync(BINARY, ["--data-dir", dir, ...args], { encoding: "utf8" });
}

function identity(dir: string): string {
  const id = distlib(dir, "whoami")
    .split("\n")
    .find((line) => line.startsWith("identity"))
    ?.split(/\s+/)[1];
  if (!id) {
    throw new Error("`distlib whoami` printed no identity");
  }
  return id;
}

/** Starts `distlib run` and resolves once its API is listening. */
function run(dir: string, args: string[]): Promise<ChildProcess> {
  const child = spawn(BINARY, ["--data-dir", dir, ...args], { stdio: ["ignore", "pipe", "pipe"] });
  let log = "";
  return new Promise((ready, failed) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      failed(new Error(`the node did not start within ${STARTUP_MS}ms:\n${log}`));
    }, STARTUP_MS);
    const hear = (chunk: Buffer) => {
      log += chunk.toString();
      if (log.includes("local api listening")) {
        clearTimeout(timer);
        ready(child);
      }
    };
    child.stdout?.on("data", hear);
    child.stderr?.on("data", hear);
    child.once("exit", (code) => {
      clearTimeout(timer);
      failed(new Error(`the node exited with ${code} before it was ready:\n${log}`));
    });
  });
}

function freeTcpPort(): Promise<number> {
  return new Promise((found, failed) => {
    const server = createServer();
    server.once("error", failed);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() =>
        typeof address === "object" && address ? found(address.port) : failed(new Error("no port")),
      );
    });
  });
}

function freeUdpPort(): Promise<number> {
  return new Promise((found, failed) => {
    const socket = createSocket("udp4");
    socket.once("error", failed);
    socket.bind(0, "127.0.0.1", () => {
      const { port } = socket.address();
      socket.close(() => found(port));
    });
  });
}

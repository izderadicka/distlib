import { describe, expect, it } from "vitest";

import { reloader } from "./reloader";

/** A load that finishes only when the test says so. */
function slowLoad() {
  const finishes: Array<() => void> = [];
  const load = () => new Promise<void>((done) => finishes.push(done));
  return { load, runs: () => finishes.length, finish: () => finishes.at(-1)?.() };
}

const settle = () => new Promise((done) => setTimeout(done, 0));

describe("reloading", () => {
  it("loads when asked", async () => {
    const slow = slowLoad();

    reloader(slow.load)();

    expect(slow.runs()).toBe(1);
  });

  it("never loads twice at once, and loads once more for any number of asks meanwhile", async () => {
    const slow = slowLoad();
    const reload = reloader(slow.load);

    reload();
    reload();
    reload();
    reload();
    expect(slow.runs()).toBe(1);

    slow.finish();
    await settle();
    // Once more, started after the last ask.
    expect(slow.runs()).toBe(2);

    slow.finish();
    await settle();
    expect(slow.runs()).toBe(2);
  });

  it("loads again when asked after it has finished", async () => {
    const slow = slowLoad();
    const reload = reloader(slow.load);
    reload();
    slow.finish();
    await settle();

    reload();

    expect(slow.runs()).toBe(2);
  });
});

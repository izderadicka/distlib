import { describe, expect, it } from "vitest";

import { bytes, instant } from "./format";

describe("writing a size", () => {
  it("uses the largest decimal unit it reaches, to one place", () => {
    expect(bytes(0)).toBe("0 B");
    expect(bytes(999)).toBe("999 B");
    expect(bytes(1000)).toBe("1 KB");
    expect(bytes(1_500_000_000)).toBe("1.5 GB");
  });

  it("stops at petabytes", () => {
    expect(bytes(2e18)).toBe("2000 PB");
  });
});

describe("reading a moment", () => {
  it("reads microseconds, as the read model keeps them", () => {
    // `last_modified` as a node answered it.
    expect(instant(1790521054348270).toISOString()).toBe("2026-09-27T14:57:34.348Z");
  });
});

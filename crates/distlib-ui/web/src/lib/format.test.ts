import { describe, expect, it } from "vitest";

import { availability, bytes, instant } from "./format";

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

describe("saying who has an item", () => {
  it("counts the online holders, and says when this node is one", () => {
    expect(availability({ held: true, providers: 2 })).toBe("held here, 2 online");
    expect(availability({ held: false, providers: 1 })).toBe("1 online");
    expect(availability({ held: false, providers: 0 })).toBe("none online");
  });

  it("does not count while anyone's holdings are unknown: the count could be short", () => {
    expect(availability({ held: true, providers: null })).toBe("held here, online holders unknown");
    expect(availability({ held: false, providers: null })).toBe("online holders unknown");
  });
});
